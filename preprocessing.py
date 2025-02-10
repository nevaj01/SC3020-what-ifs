import psycopg2
from typing import TypedDict
import networkx as nx
import ast


# Typed dictionary for details to connect to database
class LoginDetails(TypedDict):
    host: str
    user: str
    password: str
    dbname: str
    port: int


# Handles connection to database and the logic to perform database operations
class DbConnect:
    def __init__(self, login_details: LoginDetails):
        self.login_details = login_details
        self.connection = psycopg2.connect(
            host=login_details["host"],
            port=login_details["port"],
            user=login_details["user"],
            password=login_details["password"],
            dbname=login_details["dbname"]
        )


    # Returns connection to database
    def get_connection(self):
        return self.connection


    # Closes connection to database
    def close_connection(self):
        if self.connection:
            self.connection.close()


    # Retrives all relations from database
    def retrieve_tables(self):
        cursor = self.connection.cursor()
        query = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public'
        """
        cursor.execute(query)
        tables = cursor.fetchall()
        table_list = [table[0] for table in tables]
        cursor.close()
        return table_list

    # Retrives all databases in server
    def retrieve_databases(self):
        cursor = self.connection.cursor()
        query = """
        SELECT datname 
        FROM pg_database 
        WHERE datistemplate = false
        """
        cursor.execute(query)
        databases = cursor.fetchall()
        database_list = [db[0] for db in databases]
        cursor.close()
        return database_list
    
    # Retrieves the name of the currently connected database
    def retrieve_current_database(self):
        cursor = self.connection.cursor()
        query = "SELECT current_database()"
        cursor.execute(query)
        current_db = cursor.fetchone()[0]
        cursor.close()
        return current_db
    
    def connect_to_database(self, database):
        # Ensure the connection exists
        if not self.connection:
            raise Exception("No existing connection to update. Initialize a connection first.")


        # Extract connection parameters
        host = self.login_details['host']
        user = self.login_details['user']
        password = self.login_details['password']
        port = self.login_details['port']

        # Close the existing connection
        self.connection.close()

        # Reconnect with the new database
        try:
            self.connection = psycopg2.connect(
                host=host,
                user=user,
                password=password,
                port=port,
                dbname=database  # Use the new database name
            )

        except Exception as e:
            raise e



    # Retrives all columns from relation
    def retrieve_columns(self, table):
        cursor = self.connection.cursor()
        query = f"""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = '{table}'
        ORDER BY ordinal_position
        """
        cursor.execute(query)
        columns = cursor.fetchall()
        column_list = [col[0] for col in columns]
        cursor.close()
        return column_list
    

    # Retrives QEP given given query. Return QEP format varies depending on value of json.
    def retrieve_qep(self, query, json=False):
        cursor = self.connection.cursor()
        try:
            if json:
                explain_query = f"EXPLAIN (FORMAT JSON) {query}"
            else:
                explain_query = f"EXPLAIN {query}"         
            cursor.execute(explain_query)
            result = cursor.fetchall()
            if json:
                qep = result[0][0]
                qep_text = str(qep)[1:-1]  # To remove square brackets
            else:
                qep_text = "\n".join(row[0] for row in result)
        except Exception as e:
            cursor.execute("ROLLBACK;")
            raise e
        finally:
            cursor.close()
        return qep_text


    #========================================Logic to generate Procedural QEP========================================#
    def parse_plan(self, qep: dict):
        # Initialize nodes dictionary with the root node
        nodes = {'Plan': qep['Plan']['Node Type']}
        
        # Check if the root node has child plans and add them if present
        if 'Plans' in qep['Plan']:
            self.add_node(qep['Plan']['Plans'], nodes)
        
        # Ensure the involved relation (table) is included even for simple queries
        if 'Relation Name' in qep['Plan']:
            nodes['Table'] = qep['Plan']['Relation Name']
        
        return nodes


    def add_node(self, qep, nodes):
        childIndex = 0
        for plan in qep:
            # Add the current plan node
            if 'Child' not in nodes:
                nodes['Child'] = [{'Plan': plan['Node Type']}]
            else:
                nodes['Child'].append({'Plan': plan['Node Type']})
                childIndex += 1

            # Recursively add child nodes if the current plan has children
            if 'Plans' in plan:
                self.add_node(plan['Plans'], nodes['Child'][childIndex])
            
            # Include additional information if available
            if 'Relation Name' in plan:
                nodes['Child'][childIndex]['Table'] = plan['Relation Name']
            if 'Index Name' in plan:
                nodes['Child'][childIndex]['Index'] = plan['Index Name']


    def printTree(self, tree):
        nodeCount = [1]  # Initialize node count
        return self.recursivePrint(tree, None, nodeCount)  # Collect the output as a string


    def recursivePrint(self, branch, childOf, nodeCount):
        output = ""  # Start with an empty string for the output
        
        # Process child nodes first
        if 'Child' in branch:
            for plan in branch['Child']:
                output += self.recursivePrint(plan, branch, nodeCount)
        
        # Construct the output string for the current branch after children
        current_output = f"{nodeCount[0]}. {branch['Plan']}"
        
        # If the node has a direct relation (table), include it
        if 'Table' in branch:
            current_output += f" on {branch['Table']}"
        else:
            # If no direct relation, gather relations from all descendants
            involved_tables = self.get_all_relations(branch)
            if involved_tables:
                current_output += " on " + " and ".join(involved_tables)
        
        # Add parent information, including table names, if available
        if childOf:
            parent_desc = childOf['Plan']
            if 'Table' in childOf:
                parent_desc += f" on {childOf['Table']}"
            else:
                # Gather parent-related tables if not directly specified
                parent_tables = self.get_all_relations(childOf)
                if parent_tables:
                    parent_desc += " on " + " and ".join(parent_tables)
            current_output += f" (Child of {parent_desc})"
        
        # Increment node count for tracking
        nodeCount[0] += 1
        current_output += "\n"  # Newline for each node entry
        
        # Append the current node output after children
        output += current_output
        
        return output


    def get_all_relations(self, branch):
        # Collect all relations (tables) from a node and its descendants
        tables = []
        if 'Table' in branch:
            tables.append(branch['Table'])
        if 'Child' in branch:
            for child in branch['Child']:
                tables.extend(self.get_all_relations(child))
        # Remove duplicates and return the list of tables
        return list(set(tables))


    def generate_procedural_qep(self, qep_json):
        qep_dict = ast.literal_eval(qep_json)
        nodes = self.parse_plan(qep_dict)
        procedural_qep = self.printTree(nodes)
        return procedural_qep
    #================================================================================================================#

    #=======================================Logic to generate Cost Calculation=======================================#
    def explain_cost(self, qep_json):
        # Parse the JSON to a dictionary
        qep_dict = ast.literal_eval(qep_json)
        
        # Parse the plan into a structured tree
        nodes = self.parse_plan_with_costs(qep_dict)
        
        # Generate the cost output
        cost_output, total_cost = self.print_cost_tree(nodes)
        cost_output += f"\n= Total Cost: {total_cost}\n"
        
        return cost_output, total_cost


    def parse_plan_with_costs(self, qep):
        # Initialize nodes dictionary with the root node and its cost
        nodes = {'Plan': qep['Plan']['Node Type'], 'Cost': qep['Plan'].get('Total Cost', 0)}
        
        # Check if the root node has child plans and add them if present
        if 'Plans' in qep['Plan']:
            self.add_node_with_costs(qep['Plan']['Plans'], nodes)
        
        return nodes


    def add_node_with_costs(self, qep, nodes):
        child_index = 0
        for plan in qep:
            # Initialize the child node with its plan and cost
            child_node = {'Plan': plan['Node Type'], 'Cost': plan.get('Total Cost', 0)}
            
            # Add the child node to 'Child' list of nodes
            if 'Child' not in nodes:
                nodes['Child'] = [child_node]
            else:
                nodes['Child'].append(child_node)
                child_index += 1

            # Recursively process nested plans if they exist
            if 'Plans' in plan:
                self.add_node_with_costs(plan['Plans'], nodes['Child'][child_index])
            
            # Add relation (table or index) information if present
            if 'Relation Name' in plan:
                nodes['Child'][child_index]['Table'] = plan['Relation Name']
            if 'Index Name' in plan:
                nodes['Child'][child_index]['Index'] = plan['Index Name']


    def print_cost_tree(self, tree):
        node_count = [1]  # Counter to keep track of node numbers
        total_cost = 0  # Initialize total cost

        # Generate the full output with `+` signs on every line
        cost_output, total_cost = self.recursive_print_cost(tree, None, node_count, total_cost)

        # Remove the trailing `+` from the last line, if present
        if cost_output.endswith(" +\n"):
            cost_output = cost_output[:-3]  # Remove " +\n" from the last line
    
        return cost_output, total_cost


    def recursive_print_cost(self, branch, child_of, node_count, total_cost):
        output = ""  # Start with an empty string for the output
        current_cost = branch.get('Cost', 0)  # Get the cost of the current node
        total_cost += current_cost  # Add to the total cost

        # Process child nodes first
        if 'Child' in branch:
            for plan in branch['Child']:
                child_output, total_cost = self.recursive_print_cost(plan, branch, node_count, total_cost)
                output += child_output  # Append the child's output
        
        # Construct the output string for the current branch
        current_output = f"{current_cost} ({branch['Plan']}"
        if 'Table' in branch:
            current_output += f" on {branch['Table']}"
        else:
            # If no direct relation, gather relations from all descendants
            involved_tables = self.get_all_relations(branch)
            if involved_tables:
                current_output += " on " + " and ".join(involved_tables)

        # Close parentheses and add `+`
        current_output += ") +\n"
        
        # Increment node count for tracking
        node_count[0] += 1
        
        # Append the current node output after children
        output += current_output
        
        return output, total_cost
    #================================================================================================================#
    
    #========================================Logic to generate Cost Comparison=======================================#
    def compare_cost(self, qep_cost, aqp_cost):
        # Construct the initial message with the total costs of QEP and AQP
        result = (
            f"The total cost of QEP is {qep_cost}.\n"
            f"The total cost of AQP is {aqp_cost}.\n"
        )
        
        # Determine which plan is more cost-effective
        if qep_cost < aqp_cost:
            result += "The QEP is more cost-effective."
        elif aqp_cost < qep_cost:
            result += "The AQP is more cost-effective."
        else:
            result += "Both QEP and AQP have the same cost and are equally cost-effective."
        
        return result
    #================================================================================================================#
    
    #===========================================Logic to generate QEP Tree===========================================#
    def hierarchical_layout(self, G, root=None, width=1.0, vert_gap=0.2, vert_loc=0, xcenter=0.5):
        pos = self._hierarchy_pos(G, root, width, vert_gap, vert_loc, xcenter)
        return pos


    def _hierarchy_pos(self, G, node, width=1.0, vert_gap=0.2, vert_loc=0, xcenter=0.5, pos=None, parent=None, parsed=[]):
        if pos is None:
            pos = {node: (xcenter, vert_loc)}
        else:
            pos[node] = (xcenter, vert_loc)
        
        children = list(G.successors(node))
        if not isinstance(G, nx.DiGraph) and parent is not None:
            children.remove(parent)  # Remove parent for undirected graphs.

        if len(children) != 0:
            dx = width / len(children) 
            nextx = xcenter - width / 2 - dx / 2
            for child in children:
                nextx += dx
                pos = self._hierarchy_pos(G, child, width=dx, vert_gap=vert_gap, vert_loc=vert_loc-vert_gap, xcenter=nextx, pos=pos, parent=node, parsed=parsed)
        return pos


    def generate_qep_graph(self, qep_json):
        # Retrieve and parse QEP JSON data
        qep_dict = ast.literal_eval(qep_json)
        
        # Initialize directed graph
        graph = nx.DiGraph()
        
        # Capture the root node ID
        root_node_id = self.recursively_add_nodes(graph, qep_dict["Plan"])
        
        # Return both the graph and root_node_id
        return graph, root_node_id


    def recursively_add_nodes(self, graph, node, parent=None):
        # Generate a unique node ID
        node_id = node.get("Node Type", "Unknown") + "_" + str(id(node))
        
        # Filter out the 'Plans' attribute and add other attributes to the node
        node_attributes = {k: v for k, v in node.items() if k != "Plans"}
        graph.add_node(node_id, **node_attributes)
        
        # Add an edge if this is not the root node
        if parent:
            graph.add_edge(parent, node_id)
        
        # Recursively add child nodes if they exist
        for sub_plan in node.get("Plans", []):
            self.recursively_add_nodes(graph, sub_plan, node_id)
        
        # Return the root node ID when the function is called with `parent=None`
        if parent is None:
            return node_id
    #================================================================================================================#


        

