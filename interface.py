import customtkinter as ctk
import tkinter as tk
from tkinter import messagebox
import psycopg2
import networkx as nx
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.patches import FancyArrowPatch
from preprocessing import LoginDetails, DbConnect
from whatif import QueryModifier
import ast

ctk.set_appearance_mode("dark")  
ctk.set_default_color_theme("blue")  


class LoginWindow:
    def __init__(self, root):
        self.root = root
        self.root.title("PostgreSQL Connection")
        self.root.geometry("500x400")
        self.root.resizable(False, False)

        # Handle window close event to exit the mainloop
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)

        # Outer frame for alignment
        self.login_frame_main = ctk.CTkFrame(self.root, fg_color="#2b2b2b")
        self.login_frame_main.pack(fill="both", expand=True)

        # Schema frame
        self.login_frame = ctk.CTkFrame(self.login_frame_main, fg_color="#2b2b2b")
        self.login_frame.pack(expand=True)
        
        # Centered Login Title Label
        title_label = ctk.CTkLabel(self.login_frame, text="Database Login", font=("Arial", 28))
        title_label.grid(row=0, column=0, columnspan=2, padx=20, pady=(20, 10), sticky="N")

        # Centered Login Instructions Label
        instruction_label_1 = ctk.CTkLabel(self.login_frame, text="Input the following parameters to connect to the database.")
        instruction_label_1.grid(row=1, column=0, columnspan=2, padx=20, pady=(0, 0), sticky="N")
        
        instruction_label_2 = ctk.CTkLabel(self.login_frame, text="Alternatively, leave the Database field empty and select database later.")
        instruction_label_2.grid(row=2, column=0, columnspan=2, padx=20, pady=(0, 20), sticky="N")

        # Labels and input fields for connection details
        ctk.CTkLabel(self.login_frame, text="Server:").grid(row=3, column=0, padx=(50, 10), pady=5, sticky="E")
        self.server_input = ctk.CTkEntry(self.login_frame)
        self.server_input.insert(0, "localhost")
        self.server_input.grid(row=3, column=1, padx=(10, 50), pady=5, sticky="W")

        ctk.CTkLabel(self.login_frame, text="Username:").grid(row=4, column=0, padx=(50, 10), pady=5, sticky="E")
        self.user_input = ctk.CTkEntry(self.login_frame)
        self.user_input.insert(0, "postgres")
        self.user_input.grid(row=4, column=1, padx=(10, 50), pady=5, sticky="W")

        ctk.CTkLabel(self.login_frame, text="Password:").grid(row=5, column=0, padx=(50, 10), pady=5, sticky="E")
        self.password_input = ctk.CTkEntry(self.login_frame, placeholder_text="Enter your Password", show="*")
        self.password_input.grid(row=5, column=1, padx=(10, 50), pady=5, sticky="W")

        ctk.CTkLabel(self.login_frame, text="Database:").grid(row=6, column=0, padx=(50, 10), pady=5, sticky="E")
        self.db_input = ctk.CTkEntry(self.login_frame, placeholder_text="Optional")
        self.db_input.grid(row=6, column=1, padx=(10, 50), pady=5, sticky="W")

        ctk.CTkLabel(self.login_frame, text="Port:").grid(row=7, column=0, padx=(50, 10), pady=5, sticky="E")
        self.port_input = ctk.CTkEntry(self.login_frame)
        self.port_input.insert(0, "5432")
        self.port_input.grid(row=7, column=1, padx=(10, 50), pady=5, sticky="W")

        # Centered Connect button
        connect_button = ctk.CTkButton(self.login_frame, text="Connect", command=self.connect_to_db)
        connect_button.grid(row=8, column=0, columnspan=2, pady=20, sticky="N")


    def connect_to_db(self):
        login_details = {
            "host": self.server_input.get(),
            "user": self.user_input.get(),
            "password": self.password_input.get(),
            "port": int(self.port_input.get())
        }

        # Check if the database input is empty
        if self.db_input.get().strip() != "":
            login_details["dbname"] = self.db_input.get()  # Use the specified database
        else:
            login_details["dbname"] = "postgres"  # Default to 'postgres' database if empty

        try:
            dbconnect = DbConnect(login_details)
            
            # Check if a database was provided in the input
            if self.db_input.get().strip() != "":
                messagebox.showinfo("Success", f"Connected to the database {self.db_input.get()} successfully!")
            else:
                messagebox.showinfo("Success",  "Connected to the server successfully!\nNo database specified, connected to default postgres database.")

        except Exception as e:
            messagebox.showerror("Error", f"Failed to connect to database: {e}")
            return

        # Open the main window, passing the connection object
        self.open_main_window(dbconnect)


    def open_main_window(self, dbconnect):
        self.root.withdraw()  # Hide the login window
        MainWindow(self.root, dbconnect)


    def on_close(self):
        self.root.quit()  # Exit the mainloop


class MainWindow:
    def __init__(self, master, dbconnect):
        self.master = master
        self.dbconnect = dbconnect
        self.valid_configurations = None

        # Create main window
        self.window = ctk.CTkToplevel(master)
        self.window.title("QEP Explainer")
        self.window.geometry("1000x500")
        self.window.minsize(1100, 500)

        # Bind the close event of the new window
        self.window.protocol("WM_DELETE_WINDOW", self.on_close)

        # Main scrollable frame
        self.scrollable_frame = ctk.CTkScrollableFrame(self.window, width=200, height=200, fg_color="#2b2b2b")
        self.scrollable_frame.pack(fill="both", expand=True)


        #============================================Frame to Select Database============================================#
        # Outer frame for alignment
        self.database_frame_main = ctk.CTkFrame(self.scrollable_frame, width=100, height=100, corner_radius=15, fg_color="#333333")
        self.database_frame_main.pack(pady=20, padx=20, fill="both", expand=True)

        # Database frame
        self.database_frame = ctk.CTkFrame(self.database_frame_main, width=100, height=100, corner_radius=15, fg_color="#333333")
        self.database_frame.pack(expand=True)

        # For selection of database
        select_database_label = ctk.CTkLabel(self.database_frame, text="Select a database:")
        select_database_label.grid(row=0, column=0, padx=10, pady=(10,0)) 
        database_list = self.get_all_databases() 
        self.select_database_dropdown = ctk.CTkComboBox(self.database_frame, values=database_list)
        self.select_database_dropdown.grid(row=1, column=0, padx=10, pady=(0,10)) 
        select_table_button = ctk.CTkButton(self.database_frame, text="Connect", command=self.on_connect_database)
        select_table_button.grid(row=2, column=0, padx=10, pady=10)

        # For displaying connected database
        connected_database = self.dbconnect.retrieve_current_database()
        self.current_database_label = ctk.CTkLabel(self.database_frame, text=f"Connected database: {connected_database}", font=("Arial", 28))
        self.current_database_label.grid(row=0, column=1,rowspan=3, padx=100, pady=10)
        #================================================================================================================#

        #===================================Frame to Check Database Tables and Columns===================================#
        # Outer frame for alignment
        self.schema_frame_main = ctk.CTkFrame(self.scrollable_frame, width=100, height=100, corner_radius=15, fg_color="#333333")
        self.schema_frame_main.pack(pady=20, padx=20, fill="both", expand=True)

        # Schema frame
        self.schema_frame = ctk.CTkFrame(self.schema_frame_main, width=100, height=100, corner_radius=15, fg_color="#333333")
        self.schema_frame.pack(expand=True)

        # Frame title
        schema_frame_label = ctk.CTkLabel(self.schema_frame, text="Check Database Schema", font=("Arial", 28))
        schema_frame_label.grid(row=0, column=0, columnspan=2, padx=10, pady=10) 

        # For selection of relations in database
        select_table_label = ctk.CTkLabel(self.schema_frame, text="Select a table:")
        select_table_label.grid(row=1, column=0, padx=10, pady=(10,0)) 
        table_list = self.get_all_tables() 
        self.select_table_dropdown = ctk.CTkComboBox(self.schema_frame, values=table_list)
        self.select_table_dropdown.grid(row=2, column=0, padx=10, pady=(0,10)) 
        select_table_button = ctk.CTkButton(self.schema_frame, text="Select", command=self.on_select_table)
        select_table_button.grid(row=3, column=0, padx=10, pady=10)

        # For displaying columns in selected relation
        self.columns_tab_view = ctk.CTkTabview(self.schema_frame)
        self.columns_tab_view.grid(row=1, column=1, rowspan=3, padx=30, pady=10) 
        self.columns_tab_view.add("Attributes")
        self.columns_display_box =  ctk.CTkTextbox(self.columns_tab_view.tab("Attributes"), width=700, height=150)
        self.columns_display_box.grid(row=0, column=0, padx=10, pady=10) 
        #================================================================================================================#

        #===================================================Query Frame==================================================#
        # Outer frame for alignment
        self.query_frame_main = ctk.CTkFrame(self.scrollable_frame, width=100, height=100, corner_radius=15, fg_color="#333333")
        self.query_frame_main.pack(pady=20, padx=20, fill="both", expand=True)

        # Query frame
        self.query_frame = ctk.CTkFrame(self.query_frame_main, width=100, height=200, corner_radius=15, fg_color="#333333")
        self.query_frame.pack(expand=True)

        # Frame title
        query_input_label = ctk.CTkLabel(self.query_frame, text="Enter PostgreSQL Query", font=("Arial", 28))
        query_input_label.grid(row=0, column=0, padx=10, pady=10) 

        # For input of PostgreSQL query
        self.query_input_box =  ctk.CTkTextbox(self.query_frame, width=700, height=150)
        self.query_input_box.grid(row=1, column=0, padx=10, pady=10) 
        self.query_input_button = ctk.CTkButton(self.query_frame, text="Submit Query", command=self.on_submit_query)
        self.query_input_button.grid(row=2, column=0, padx=10, pady=10)

        # For viewing of results 
        self.query_result_tab_view = ctk.CTkTabview(self.query_frame)
        self.query_result_tab_view.grid(row=3, column=0, padx=10, pady=10) 
        self.query_result_tab_view.add("QEP")
        self.query_result_tab_view.add("Procedural QEP")
        self.query_result_tab_view.add("QEP Tree")
        self.query_result_tab_view.add("QEP Cost Calculation")
        self.qep_display_box =  ctk.CTkTextbox(self.query_result_tab_view.tab("QEP"), width=700, height=150)
        self.qep_display_box.pack(padx=10, pady=10)
        self.procedural_qep_display_box =  ctk.CTkTextbox(self.query_result_tab_view.tab("Procedural QEP"), width=700, height=150)
        self.procedural_qep_display_box.pack(padx=10, pady=10)
        self.qep_graph_frame = ctk.CTkFrame(self.query_result_tab_view.tab("QEP Tree"), width=700, height=400, fg_color="#2b2b2b")
        self.qep_graph_frame.pack(padx=10, pady=10)
        self.qep_cost_box =  ctk.CTkTextbox(self.query_result_tab_view.tab("QEP Cost Calculation"), width=700, height=150)
        self.qep_cost_box.pack(padx=10, pady=10)
        #================================================================================================================#

        #====================================================AQP Frame===================================================#
        # Outer frame for alignment
        self.aqp_frame_main = ctk.CTkFrame(self.scrollable_frame, width=100, height=100, corner_radius=15, fg_color="#333333")
        self.aqp_frame_main.pack(pady=20, padx=20, fill="both", expand=True)

        # AQP frame
        self.aqp_frame = ctk.CTkFrame(self.aqp_frame_main, width=100, height=200, corner_radius=15, fg_color="#333333")
        self.aqp_frame.pack(expand=True)

        # Frame title
        configs_select_label = ctk.CTkLabel(self.aqp_frame, text="Select Configurations", font=("Arial", 28))
        configs_select_label.grid(row=0, column=0, columnspan=4, padx=10, pady=10) 

        # For showing valid configurations
        self.valid_configurations_tab_view = ctk.CTkTabview(self.aqp_frame)
        self.valid_configurations_tab_view.grid(row=1, column=0, columnspan=4, padx=10, pady=10) 
        self.valid_configurations_tab_view.add("Valid Combinations")
        self.valid_configurations_display_box =  ctk.CTkTextbox(self.valid_configurations_tab_view.tab("Valid Combinations"), width=700, height=150)
        self.valid_configurations_display_box.pack(padx=10, pady=10)

        # For selecting scan options
        scan_options_label = ctk.CTkLabel(self.aqp_frame, text="Select Scan Options", font=("Arial", 14))
        scan_options_label.grid(row=2, column=0, columnspan=4, padx=10, pady=10)
        self.bitmapscan_switch_var = ctk.BooleanVar(value=True)
        self.indexscan_switch_var = ctk.BooleanVar(value=True)
        self.indexonlyscan_switch_var = ctk.BooleanVar(value=True)
        self.seqscan_switch = ctk.BooleanVar(value=True)
        self.bitmapscan_switch = ctk.CTkSwitch(self.aqp_frame, text="Bitmap Scan", variable=self.bitmapscan_switch_var, command=self.update_button)
        self.bitmapscan_switch.grid(row=3, column=0, padx=10, pady=10)
        self.indexscan_switch = ctk.CTkSwitch(self.aqp_frame, text="Index Scan", variable=self.indexscan_switch_var, command=self.update_button)
        self.indexscan_switch.grid(row=3, column=1, padx=10, pady=10)
        self.indexonlyscan_switch = ctk.CTkSwitch(self.aqp_frame, text="Index Only Scan", variable=self.indexonlyscan_switch_var, command=self.update_button)
        self.indexonlyscan_switch.grid(row=3, column=2, padx=10, pady=10)
        self.seqscan_switch = ctk.CTkSwitch(self.aqp_frame, text="Sequential Scan", variable=self.seqscan_switch, command=self.update_button)
        self.seqscan_switch.grid(row=3, column=3, padx=10, pady=10)

        # For selecting join options
        join_options_label = ctk.CTkLabel(self.aqp_frame, text="Select Join Options", font=("Arial", 14))
        join_options_label.grid(row=4, column=0, columnspan=4, padx=10, pady=10)
        self.hashjoin_switch_var = ctk.BooleanVar(value=True)
        self.mergejoin_switch_var = ctk.BooleanVar(value=True)
        self.nestloop_switch_var = ctk.BooleanVar(value=True)
        self.hashjoin_switch = ctk.CTkSwitch(self.aqp_frame, text="Hash Join", variable=self.hashjoin_switch_var, command=self.update_button)
        self.hashjoin_switch.grid(row=5, column=0, padx=10, pady=10)
        self.mergejoin_switch = ctk.CTkSwitch(self.aqp_frame, text="Merge Join", variable=self.mergejoin_switch_var, command=self.update_button)
        self.mergejoin_switch.grid(row=5, column=1, padx=10, pady=10)
        self.nestloop_switch = ctk.CTkSwitch(self.aqp_frame, text="Nest Loop", variable=self.nestloop_switch_var, command=self.update_button)
        self.nestloop_switch.grid(row=5, column=2, padx=10, pady=10)

        # For selecting aggregate options
        aggregate_options_label = ctk.CTkLabel(self.aqp_frame, text="Select Aggregate Options", font=("Arial", 14))
        aggregate_options_label.grid(row=6, column=0, columnspan=4, padx=10, pady=10)
        self.hashagg_switch_var = ctk.BooleanVar(value=True)
        self.presorted_aggregate_switch_var = ctk.BooleanVar(value=True)
        self.hashagg_switch = ctk.CTkSwitch(self.aqp_frame, text="Hash Aggregate", variable=self.hashagg_switch_var, command=self.update_button)
        self.hashagg_switch.grid(row=7, column=0, padx=10, pady=10)
        self.presorted_aggregate_switch = ctk.CTkSwitch(self.aqp_frame, text="Presorted Aggregate", variable=self.presorted_aggregate_switch_var, command=self.update_button)
        self.presorted_aggregate_switch.grid(row=7, column=1, padx=10, pady=10)

        # For selecting sort options
        sort_options_label = ctk.CTkLabel(self.aqp_frame, text="Select Sort Options", font=("Arial", 14))
        sort_options_label.grid(row=8, column=0, columnspan=4, padx=10, pady=10)
        self.incremental_sort_switch_var = ctk.BooleanVar(value=True)
        self.sort_switch_var = ctk.BooleanVar(value=True)
        self.incremental_sort_switch = ctk.CTkSwitch(self.aqp_frame, text="Incremental Sort", variable=self.incremental_sort_switch_var, command=self.update_button)
        self.incremental_sort_switch.grid(row=9, column=0,  padx=10, pady=10)
        self.sort_switch = ctk.CTkSwitch(self.aqp_frame, text="Sort", variable=self.sort_switch_var, command=self.update_button)
        self.sort_switch.grid(row=9, column=1,  padx=10, pady=10)

        # For submitting configurations
        self.modified_query_button = ctk.CTkButton(self.aqp_frame, text="Submit Configurations", command=self.on_submit_configs)
        self.modified_query_button.grid(row=10, column=0, columnspan=4, padx=10, pady=10) 
        self.invalid_configuration_label = ctk.CTkLabel(self.aqp_frame, text="Invalid Combination of Configurations", font=("Arial", 14),text_color="red")
        self.invalid_configuration_label.grid(row=11, column=0, columnspan=4) # Only shown for invalid configurations
        self.invalid_configuration_label.grid_forget()

        # For viewing results
        self.aqp_result_tab_view = ctk.CTkTabview(self.aqp_frame)
        self.aqp_result_tab_view.grid(row=12, column=0, columnspan=4, padx=10, pady=10) 
        self.aqp_result_tab_view.add("AQP")
        self.aqp_result_tab_view.add("Procedural AQP")
        self.aqp_result_tab_view.add("Modified SQL Query")
        self.aqp_result_tab_view.add("AQP Tree")
        self.aqp_result_tab_view.add("AQP Cost Calculation")
        self.aqp_result_tab_view.add("Cost Comparison")
        self.aqp_display_box =  ctk.CTkTextbox(self.aqp_result_tab_view.tab("AQP"), width=700, height=150)
        self.aqp_display_box.pack(padx=10, pady=10)
        self.procedural_aqp_display_box =  ctk.CTkTextbox(self.aqp_result_tab_view.tab("Procedural AQP"), width=700, height=150)
        self.procedural_aqp_display_box.pack(padx=10, pady=10)
        self.modified_sql_query_display_box =  ctk.CTkTextbox(self.aqp_result_tab_view.tab("Modified SQL Query"), width=700, height=150)
        self.modified_sql_query_display_box.pack(padx=10, pady=10)
        self.aqp_graph_frame = ctk.CTkFrame(self.aqp_result_tab_view.tab("AQP Tree"), width=700, height=400, fg_color="#2b2b2b")
        self.aqp_graph_frame.pack(padx=10, pady=10)
        self.aqp_cost_box =  ctk.CTkTextbox(self.aqp_result_tab_view.tab("AQP Cost Calculation"), width=700, height=150)
        self.aqp_cost_box.pack(padx=10, pady=10)
        self.cost_comparison_box =  ctk.CTkTextbox(self.aqp_result_tab_view.tab("Cost Comparison"), width=700, height=150)
        self.cost_comparison_box.pack(padx=10, pady=10)
        #================================================================================================================#

        # Close button outside of scrollabe frame
        close_button = ctk.CTkButton(self.window, text="Close", command=self.on_close)
        close_button.pack(pady=10)

        
    # Connects to selected database
    def on_connect_database(self):
        selected_database = self.select_database_dropdown.get()
        try:
            self.dbconnect.connect_to_database(selected_database)
            self.current_database_label.configure(text=f"Connected database: {selected_database}")

            # Resets everything else
            new_tables = self.get_all_tables()
            self.select_table_dropdown.configure(values=new_tables)
            self.select_table_dropdown.set(new_tables[0])
            self.columns_display_box.delete("1.0", "end")
            self.query_input_box.delete("1.0", "end") 
            self.qep_display_box.delete("1.0", "end")
            self.procedural_qep_display_box.delete("1.0", "end")
            self.destroy_canvas_in_frame(self.qep_graph_frame)
            self.qep_cost_box.delete("1.0", "end")
            self.valid_configurations_display_box.delete("1.0", "end")
            self.bitmapscan_switch.select()
            self.indexscan_switch.select()
            self.indexonlyscan_switch.select()
            self.seqscan_switch.select()
            self.hashjoin_switch.select()
            self.mergejoin_switch.select()
            self.nestloop_switch.select()
            self.hashagg_switch.select()
            self.presorted_aggregate_switch.select()
            self.incremental_sort_switch.select()
            self.sort_switch.select()
            self.modified_query_button.configure(state='normal', fg_color="#1f6aa5")
            self.invalid_configuration_label.grid_forget()
            self.aqp_display_box.delete("1.0", "end")
            self.procedural_aqp_display_box.delete("1.0", "end")
            self.modified_sql_query_display_box.delete("1.0", "end")
            self.destroy_canvas_in_frame(self.aqp_graph_frame)
            self.aqp_cost_box.delete("1.0", "end")
            self.cost_comparison_box.delete("1.0", "end")

        except psycopg2.Error as e:
            messagebox.showerror("Error", f"Failed to connect to database {selected_database}.")


    # Displays results in the tabs in the QEP frame.
    def on_submit_query(self):
        # Gets original input query
        query = self.query_input_box.get("1.0", "end-1c")

        # Handle empty query
        if not query.strip(): 
             messagebox.showerror("Error", f"Query is empty.")
             return

        try:
            # Updates the QEP tab in the QEP frame
            qep = self.get_qep(query)  
            self.qep_display_box.delete("1.0", "end")
            self.qep_display_box.insert("1.0", qep)

            # Updates the Procedural QEP tab in the QEP frame
            qep_json = self.get_qep(query, True)  
            procedural_qep = self.dbconnect.generate_procedural_qep(qep_json) 
            self.procedural_qep_display_box.delete("1.0", "end")
            self.procedural_qep_display_box.insert("1.0", procedural_qep)

            # Updates the QEP Tree tab in the QEP frame
            qep_graph, root_node_id = self.dbconnect.generate_qep_graph(qep_json)
            self.visualise_qep_graph(qep_graph, root_node_id, self.qep_graph_frame)

            # Updates the QEP Cost Calculation tab in the QEP frame
            qep_cost_explanation, _  = self.dbconnect.explain_cost(qep_json)
            self.qep_cost_box.delete("1.0", "end")
            self.qep_cost_box.insert("1.0", qep_cost_explanation)

            # Generate a list of all valid combinations of configurations and store them
            qep_dict = ast.literal_eval(qep_json)
            query_modifier = QueryModifier(self.dbconnect.get_connection())
            plans = query_modifier.retrieve_all_plans(query, qep_dict)
            valid_configs = query_modifier.retrieve_valid_combinations(plans)
            self.valid_configurations = valid_configs

            # Updates the Valid Combinations tab in the AQP frame
            valid_combinations_text = query_modifier.parse_valid_configurations(self.valid_configurations)
            self.valid_configurations_display_box.delete("1.0", "end")
            self.valid_configurations_display_box.insert("1.0", valid_combinations_text)

            # Resets AQP frame
            self.bitmapscan_switch.select()
            self.indexscan_switch.select()
            self.indexonlyscan_switch.select()
            self.seqscan_switch.select()
            self.hashjoin_switch.select()
            self.mergejoin_switch.select()
            self.nestloop_switch.select()
            self.hashagg_switch.select()
            self.presorted_aggregate_switch.select()
            self.incremental_sort_switch.select()
            self.sort_switch.select()
            self.modified_query_button.configure(state='normal', fg_color="#1f6aa5")
            self.invalid_configuration_label.grid_forget()
            self.aqp_display_box.delete("1.0", "end")
            self.procedural_aqp_display_box.delete("1.0", "end")
            self.modified_sql_query_display_box.delete("1.0", "end")
            self.destroy_canvas_in_frame(self.aqp_graph_frame)
            self.aqp_cost_box.delete("1.0", "end")
            self.cost_comparison_box.delete("1.0", "end")
        except Exception as e:
            print(f"Error: {e}")
        


    # Displays results in the tabs in the AQP frame.
    def on_submit_configs(self):
        # Gets original input query
        query = self.query_input_box.get("1.0", "end-1c")

        # Handle empty query
        if not query.strip(): 
             messagebox.showerror("Error", f"Query is empty.")

        # Get boolean values of switches
        bitmapscan_bool = self.bitmapscan_switch_var.get()
        indexscan_bool = self.indexscan_switch_var.get()
        indexonlyscan_bool = self.indexonlyscan_switch_var.get()
        seqscan_bool = self.seqscan_switch.get()
        hashjoin_bool = self.hashjoin_switch_var.get()
        mergejoin_bool = self.mergejoin_switch_var.get()
        nestloop_bool =  self.nestloop_switch_var.get()
        hashagg_bool = self.hashagg_switch_var.get()
        presorted_aggregate_bool = self.presorted_aggregate_switch_var.get()
        incremental_sort_bool = self.incremental_sort_switch_var.get()
        sort_bool = self.sort_switch_var.get()

        # Creates list of configs
        configs = [bitmapscan_bool, indexscan_bool, indexonlyscan_bool, seqscan_bool, hashjoin_bool, mergejoin_bool, nestloop_bool, hashagg_bool, presorted_aggregate_bool, incremental_sort_bool, sort_bool]

        try:
            # Updates the AQP tab in AQP Frame
            query_modifier = QueryModifier(self.dbconnect.get_connection())
            modified_query, aqp_text = query_modifier.get_aqp_and_query(query, configs)
            self.aqp_display_box.delete("1.0", "end")
            self.aqp_display_box.insert("1.0", aqp_text)

            # Updates the Modified SQL Query tab in the AQP Frame
            parsed_query = query_modifier.parse_query(modified_query)
            self.modified_sql_query_display_box.delete("1.0", "end")
            self.modified_sql_query_display_box.insert("1.0", parsed_query)

            # Updates the Procedural AQP tab in the AQP Frame
            _ ,  aqp_json = query_modifier.get_aqp_and_query(query, configs, True)
            procedural_aqp = self.dbconnect.generate_procedural_qep(aqp_json) 
            self.procedural_aqp_display_box.delete("1.0", "end")
            self.procedural_aqp_display_box.insert("1.0", procedural_aqp)

            # Updates the AQP Tree tab in the AQP Frame
            aqp_graph, root_node_id = self.dbconnect.generate_qep_graph(aqp_json)
            self.visualise_qep_graph(aqp_graph, root_node_id, self.aqp_graph_frame)

            # Updates the AQP Cost Cauculation tab in the AQP Frame
            aqp_cost_explanation, aqp_cost = self.dbconnect.explain_cost(aqp_json)
            self.aqp_cost_box.delete("1.0", "end")
            self.aqp_cost_box.insert("1.0", aqp_cost_explanation)
            qep_json = self.get_qep(query, True)
            _ , qep_cost = self.dbconnect.explain_cost(qep_json)

            # Updates the Cost Comparison tab in the AQP Frame
            cost_comparison = self.dbconnect.compare_cost(qep_cost, aqp_cost)
            self.cost_comparison_box.delete("1.0", "end")
            self.cost_comparison_box.insert("1.0", cost_comparison)

        except Exception as e:
            print(f"Error: {e}")

    # For updating state of button to disallow invalid combinations of configurations
    def update_button(self):
        # If query habs not been entered yet
        if self.valid_configurations == None:
            return

        # Get boolean values of switches
        bitmapscan_bool = self.bitmapscan_switch_var.get()
        indexscan_bool = self.indexscan_switch_var.get()
        indexonlyscan_bool = self.indexonlyscan_switch_var.get()
        seqscan_bool = self.seqscan_switch.get()
        hashjoin_bool = self.hashjoin_switch_var.get()
        mergejoin_bool = self.mergejoin_switch_var.get()
        nestloop_bool =  self.nestloop_switch_var.get()
        hashagg_bool = self.hashagg_switch_var.get()
        presorted_aggregate_bool = self.presorted_aggregate_switch_var.get()
        incremental_sort_bool = self.incremental_sort_switch_var.get()
        sort_bool = self.sort_switch_var.get()

        # Creates list of configs
        selected_configs = [bitmapscan_bool, indexscan_bool, indexonlyscan_bool, seqscan_bool, hashjoin_bool, mergejoin_bool, nestloop_bool, hashagg_bool, presorted_aggregate_bool, incremental_sort_bool, sort_bool]
        if selected_configs not in self.valid_configurations:
            self.modified_query_button.configure(state='disabled', fg_color="grey")
            self.invalid_configuration_label.grid(row=11, column=0, columnspan=4) 
        else:
            self.modified_query_button.configure(state='normal', fg_color="#1f6aa5")
            self.invalid_configuration_label.grid_forget()


    def visualise_qep_graph(self, graph, root_node_id, canvas_frame):
        # Destroy any existing widgets in the frame
        self.destroy_canvas_in_frame(canvas_frame)

        # Create a scrollable canvas within the frame
        content_frame = self.create_scrollable_canvas(canvas_frame)

        # Calculate positions with layout (relative positions)
        pos = self.dbconnect.hierarchical_layout(graph, root=root_node_id)
        
        # Define the canvas or figure dimensions (absolute size you want to occupy)
        canvas_width = 800  # Adjust as needed
        canvas_height = 600  # Adjust as needed
        scaling_factor = 0.2  # Adjust this scaling factor as needed to control the size of the graph

        # Convert relative positions to absolute positions with scaling
        pos = {node: (x * canvas_width * scaling_factor, y * canvas_height * scaling_factor) for node, (x, y) in pos.items()}
        
        # Calculate bounding box based on layout positions
        x_vals = [x for x, y in pos.values()]
        y_vals = [y for x, y in pos.values()]
        width = max(x_vals) - min(x_vals)
        height = max(y_vals) - min(y_vals)
        
        # Set figure size to match the bounding box, with some padding
        fig_width = width * 0.15 + 1 
        fig_height = height * 0.15 + 1
        
        # Enforce minimum width and height
        min_width = 20
        min_height = 15
        fig_width = max(fig_width, min_width)
        fig_height = max(fig_height, min_height)
        
        # Create figure with enforced minimum size
        fig, ax = plt.subplots(figsize=(fig_width, fig_height))
        
        # Draw nodes with adjusted node size
        nx.draw_networkx_nodes(graph, pos, ax=ax, node_size=1000, node_color="lightblue", edgecolors="black")
        labels = {
            node: "\n".join(f"{k}: {v}" for k, v in attrs.items() if k != "Plans")
            for node, attrs in graph.nodes(data=True)
        }
        for node, (x, y) in pos.items():
            ax.text(
                x, y, labels[node],
                ha='center', va='center',
                bbox=dict(boxstyle="round,pad=0.3", edgecolor="black", facecolor="lightblue")
            )
        
        # Draw edges and add arrow heads
        nx.draw_networkx_edges(graph, pos, ax=ax, arrows=False)
        for u, v in graph.edges():
            x1, y1 = pos[u]
            x2, y2 = pos[v]
            mid_x, mid_y = (x1 + x2) / 2, (y1 + y2) / 2
            dx, dy = x1 - x2, y1 - y2
            arrow = FancyArrowPatch((mid_x, mid_y), (mid_x + dx * 0.01, mid_y + dy * 0.01), 
                                    connectionstyle="arc3", arrowstyle="-|>", color="black", mutation_scale=15)
            ax.add_patch(arrow)    
        
        ax.axis("off")
        
        # Embed the matplotlib figure in the scrollable content frame
        canvas = FigureCanvasTkAgg(fig, master=content_frame)
        canvas.draw()
        canvas.get_tk_widget().pack(fill="both", expand=True)

        # Update the scroll region after adding the figure
        content_frame.update_idletasks()
        content_frame.update()

        return canvas


    # Destroys all widgets in the specified frame
    def destroy_canvas_in_frame(self, frame):
        for widget in frame.winfo_children():
            widget.pack_forget()  # Unpack from the frame
            widget.destroy()       # Destroy the widget


    # Gets QEP given query. Return QEP format varies depending on value of json.
    def get_qep(self, query, json=False):
        try:
            qep_text = self.dbconnect.retrieve_qep(query, json)
            return qep_text
        except psycopg2.Error as e:
            messagebox.showerror("Error", "Failed to retrieve QEP.")
    

    # Displays columns of selected relation in the Schema frame
    def on_select_table(self):
        selected_table =  self.select_table_dropdown.get()
        column_list = self.get_table_columns(selected_table)
        column_list_text = ", ".join(column_list)
        self.columns_display_box.delete("1.0", "end")
        self.columns_display_box.insert("1.0", column_list_text)


    # Get all databases in server
    def get_all_databases(self):
        try:
            databases = self.dbconnect.retrieve_databases()
            return databases
        except psycopg2.Error as e:
            messagebox.showerror("Error", "Failed to retrieve databases from server.")


    # Get all columns in relation
    def get_all_tables(self):
        try:
            tables = self.dbconnect.retrieve_tables()
            return tables
        except psycopg2.Error as e:
            messagebox.showerror("Error", "Failed to retrieve tables from database.")

    # Get all relations in database
    def get_table_columns(self, table):
        try:
            columns = self.dbconnect.retrieve_columns(table)
            return columns
        except psycopg2.Error as e:
            messagebox.showerror("Error", f"Failed to retrieve columns from {table}.")


    # Handles closing main window
    def on_close(self):
        # Display a confirmation popup
        confirm = messagebox.askyesno("Confirm Exit", "Are you sure you want to disconnect from the database?")
        
        # Proceed with closing if the user confirms
        if confirm:
            self.dbconnect.close_connection() # Close the database connection
            self.window.destroy() # Destroy the Toplevel window
            self.master.deiconify()  # Show the master window again


    def create_scrollable_canvas(self, frame):
        # Add vertical and horizontal scrollbars linked to the canvas
        v_scrollbar = tk.Scrollbar(frame, orient="vertical")
        v_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        h_scrollbar = tk.Scrollbar(frame, orient="horizontal")
        h_scrollbar.pack(side=tk.BOTTOM, fill=tk.X)

        # Create a canvas for scrolling and link it to the scrollbars
        canvas = tk.Canvas(frame, width=1000, height=600, bg="#2B2B2B", highlightthickness=0,
                        yscrollcommand=v_scrollbar.set, xscrollcommand=h_scrollbar.set)
        canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        # Configure scrollbars to control canvas scrolling
        v_scrollbar.config(command=canvas.yview)
        h_scrollbar.config(command=canvas.xview)

        # Create an internal frame within the canvas to hold content
        content_frame = tk.Frame(canvas, bg="#2b2b2b")
        canvas.create_window((0, 0), window=content_frame, anchor="nw")

        # Update scroll region based on the content size
        def update_scrollregion(event=None):
            canvas.configure(scrollregion=canvas.bbox("all"))
           

        # Bind configuration event to update scroll region
        content_frame.bind("<Configure>", update_scrollregion)

        # Initial call to set scroll region
        update_scrollregion()

        return content_frame
