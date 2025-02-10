import itertools
import re

# Handles the processing of 'what if' queries
class QueryModifier:
    def __init__(self, connection):
        self.connection = connection


    # Logic to generate AQP and corresponding PostgreSQL query given original query and list of configurations
    def get_aqp_and_query(self, query, configs, json=False):
        config_names = [
            'enable_bitmapscan',
            'enable_indexscan',
            'enable_indexonlyscan',
            'enable_seqscan',
            'enable_hashjoin',
            'enable_mergejoin',
            'enable_nestloop',
            'enable_hashagg',
            'enable_presorted_aggregate',
            'enable_incremental_sort',
            'enable_sort'
        ]
        config_queries = [
            f"SET {config_name} TO FALSE;" for config, config_name in zip(configs, config_names) if not config
        ]
        
        settings_query = "BEGIN; " + " ".join(config_queries)
        if json:
            explain_query = f"EXPLAIN (FORMAT JSON) {query};"
        else:
            explain_query = f"EXPLAIN {query};"
        combined_query = settings_query + " " + explain_query  # To execute
        display_query = settings_query + " " + query  # To display
        with self.connection.cursor() as cursor:
            try:
                cursor.execute(combined_query)
                result = cursor.fetchall()
                if json:
                    aqp = result[0][0]
                    aqp_text = str(aqp)[1:-1]
                else:
                    aqp_text = "\n".join(row[0] for row in result)   
            except Exception as e:
                cursor.execute("ROLLBACK;")
                raise e           
            finally:
                cursor.execute("ROLLBACK;")    
        return display_query, aqp_text


    #Formats query for display
    def parse_query(self, query):
        statements = re.split(r'(;)', query)
        formatted_query = "\n".join(statement.strip() + ";" for statement in statements if statement.strip() and statement != ";")
        return formatted_query

    #==========================Logic to generate all possible combinations of configurations=========================#
    def retrieve_all_plans(self, inputQuery, qep):
        # Configurations in order of execution
        scan_config = ['bitmapscan', 'indexscan', 'indexonlyscan', 'seqscan']
        other_config = ['hashjoin', 'mergejoin', 'nestloop', 'hashagg', 'presorted_aggregate', 'incremental_sort', 'sort']

        plan_list = [qep]
        config_list = [[True for _ in range(11)]]
        l = [True, False]
        
        # Generate all 4-bit combinations for scan configurations, excluding all-True combination
        combinations4 = [list(i) for i in itertools.product(l, repeat=4)]
        combinations4.remove([True for _ in range(4)])

        # Testing scan configurations
        for config in combinations4:
            query = 'BEGIN;' + ''.join(
                f'SET enable_{setting} TO FALSE;' for setting, enabled in zip(scan_config, config) if not enabled
            ) + f'EXPLAIN (FORMAT JSON) {inputQuery};'
            
            with self.connection.cursor() as cursor:
                try:
                    cursor.execute(query)
                    aqp = self.parse_plan(cursor.fetchall()[0][0][0])
                except Exception:
                    aqp = None
                finally:
                    cursor.execute("ROLLBACK;")

            if aqp and aqp not in plan_list:
                plan_list.append(aqp)
                config_list.append(config + [True for _ in range(7)])

        # Iteratively modify configurations for joins, aggregation, sorting
        plan_iterate = plan_list.copy()
        config_iterate = config_list.copy()
        
        while True:
            new_plans, new_configs = [], []

            for plan, config in zip(plan_iterate, config_iterate):
                if 'Join' not in str(plan) and 'Aggregate' not in str(plan) and 'Sort' not in str(plan):
                    continue

                # Generate combinations for other configurations based on plan contents
                combinations7 = [list(i) for i in itertools.product(l, repeat=7) if i not in config_list]
                if 'Join' not in str(plan):
                    combinations7 = [comb for comb in combinations7 if False not in comb[:3]]
                if 'Aggregate' not in str(plan):
                    combinations7 = [comb for comb in combinations7 if False not in comb[3:5]]
                if 'Sort' not in str(plan):
                    combinations7 = [comb for comb in combinations7 if False not in comb[5:]]

                # Execute configurations
                for config2 in combinations7:
                    query = 'BEGIN;' + ''.join(
                        f'SET enable_{setting} TO FALSE;' for setting, enabled in zip(scan_config, config[:4]) if not enabled
                    ) + ''.join(
                        f'SET enable_{setting} TO FALSE;' for setting, enabled in zip(other_config, config2) if not enabled
                    ) + f'EXPLAIN (FORMAT JSON) {inputQuery};'

                    with self.connection.cursor() as cursor:
                        try:
                            cursor.execute(query)
                            aqp = self.parse_plan(cursor.fetchall()[0][0][0])
                        except Exception:
                            aqp = None
                        finally:
                            cursor.execute("ROLLBACK;")

                    if aqp and aqp not in plan_list and aqp not in new_plans:
                        new_plans.append(aqp)
                        new_configs.append(config[:4] + config2)
        
            if new_plans:
                plan_iterate = new_plans.copy()
                config_iterate = new_configs.copy()
                plan_list.extend(new_plans)
                config_list.extend(new_configs)
            else:
                break
            
        return [{'aqp': plan, 'config': config} for plan, config in zip(plan_list, config_list)]


    def parse_plan(self, qep):
        nodes = {'Plan': qep.get('Plan', {}).get('Node Type')}
        self.add_node(qep.get('Plan', {}).get('Plans', []), nodes)
        return nodes


    def add_node(self, qep, nodes):
        childIndex = 0
        for plan in qep:
            if 'Child' not in nodes:
                nodes['Child'] = [{'Plan': plan.get('Node Type')}]
            else:
                nodes['Child'].append({'Plan': plan.get('Node Type')})
                childIndex += 1
            if 'Plans' in plan:
                self.add_node(plan['Plans'], nodes['Child'][childIndex])
            
            if 'Relation Name' in plan:
                nodes['Child'][childIndex]['Table'] = plan['Relation Name']
            if 'Index Name' in plan:
                nodes['Child'][childIndex]['Index'] = plan['Index Name']
    #================================================================================================================#

    # Extracts all valid configurations
    def retrieve_valid_combinations(self, plan):
            return [entry['config'] for entry in plan if 'config' in entry]
        

    # Parse and formats list of valid combinations for display
    def parse_valid_configurations(self, valid_configurations):
        # Define the features corresponding to each index
        features = [
            "Bitmap Scan", "Index Scan", "Index Only Scan", "Sequential Scan",  # Scan Options
            "Hash Join", "Merge Join", "Nest Loop",  # Join Options
            "Hash Aggregate", "Presorted Aggregate",  # Aggregate Options
            "Incremental Sort", "Sort"  # Sort Options
        ]
        
        # Initialize an empty string to accumulate the output
        output_text = ""
        
        # Iterate through the valid configurations
        for idx, config in enumerate(valid_configurations):
            output_text += f"Combination {idx + 1}:\n"
            
            # Check each feature and append the result to the output string
            for i, is_enabled in enumerate(config):
                status = "Enabled" if is_enabled else "Disabled"
                output_text += f"{features[i]}: {status}\n"
            output_text += "\n"  # Add a newline between combinations
        
        # Return the formatted output as text
        return output_text

        
