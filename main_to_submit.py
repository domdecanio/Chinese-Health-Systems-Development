import pandas as pd
import sqlalchemy as sqla


# First, we must access the MySQL server in order to access the pre-processed data.
host_name = "localhost"
host_ip = "127.0.0.1"
port = "3306"

user_id = "root"
pwd = "your_password"
db_name = "etl_dw"


def get_sqlalchemy_dataframe(user_id, pwd, host_name, db_name, sql_query):
    conn_str = f"mysql+pymysql://{user_id}:{pwd}@{host_name}/{db_name}"
    sqlEngine = sqla.create_engine(conn_str, pool_recycle=3600)
    connection = sqlEngine.connect()
    dframe = pd.read_sql(sql_query, connection);
    connection.close()

    return dframe


try:
    # Loading prepared tables from the etl_dw database on my MySQL server into PANDAS dataframes.
    etl_hos_query = "SELECT * FROM etl_hos"
    hos_df = get_sqlalchemy_dataframe(user_id, pwd, host_name, db_name, etl_hos_query)

    etl_epi_query = "SELECT * FROM etl_epi"
    epi_df = get_sqlalchemy_dataframe(user_id, pwd, host_name, db_name, etl_epi_query)

except:
    print("Error: unable to fetch data")


# Next I will create a list of all unique provinces in the dataset. This is the first
# step in order to create dataframes to describe each province.
hos_province_names = list(hos_df["Province"].unique())
hos_len = list(range(0, len(hos_province_names)))

epi_province_names = list(epi_df["Province"].unique())
epi_len = list(range(0, len(epi_province_names)))


# This is a verification step to ensure that both lists-which are of the same length-
# do not include different province names.
hos_prov_names_df = pd.DataFrame(hos_province_names, columns=["hos_names"])
hos_names_index_df = pd.DataFrame(hos_len, columns=["index"])
epi_prov_names_df = pd.DataFrame(epi_province_names, columns=["epi_names"])
epi_names_index_df = pd.DataFrame(epi_len, columns=["index"])

hos_check_df = pd.concat([hos_names_index_df, hos_prov_names_df], axis=1)
epi_check_df = pd.concat([epi_names_index_df, epi_prov_names_df], axis=1)
check_df = hos_check_df.merge(epi_check_df, on='index', how='outer')
# The following code returns "False" when printed, indicating that all province
# names are captured by both lists.
check_df.isnull().values.any()
# Renaming one of the lists for clarity of code:
province_names = hos_province_names

#

# Summary of data file ingestion:
# Because of the complexity of this data manipulation, it is more logical to
# include more metrics besides number of rows and columns in this summary.
print('Summary of ingested data:')
print(f'Number of provinces represented: {len(province_names)}')
print(f'Number of hospitals in the dataset: {hos_df.shape[0]}')
print(f'Number of epidemiological centers in the dataset: {epi_df.shape[0]}')
print(f'Number of metrics per hospital: {hos_df.shape[1]}')
print(f'Number of metrics per epidemiological center: {epi_df.shape[1]}')


# Creating the class "province" to store data pertaining to each province:
class Province:
    _registry = []

    def __init__(self, name):
        self._registry.append(self)
        self.name = name
        self.hos_development = hos_df[hos_df.Province == f'{name}']
        self.epi_development = epi_df[epi_df.Province == f'{name}']
        self.hos_dev_clean = 0
        self.epi_dev_clean = 0


# Looping through the province names to create province objects:
province_list = []
for i in province_names:
    new_prov = Province(i)
    province_list.append(new_prov)

time = list(range(1950, 1988))

# The province is the highest level of the loop.
for j in province_list:
    target_hos = []
    target_bed = []
    target_top = []
    target_tos = []
    target_day = []

    # Resetting the dataframe index:
    j.hos_development = j.hos_development.reset_index()

    # Creating a cumulative column for the province's number of hospitals:
    for i in range(1950, 1988):
        counter = 0
        for index, each in j.hos_development.iterrows():
            if each['Year_Founded'] <= i:
                counter += 1
        target_hos.append(counter)

    # Creating columns aggregating the province's cumulative number of: hospital beds,
    # hospital personnel, specialized hospital staff, and patient days.
    columns = ['Beds', 'Total_Personnel', 'Specialized_Staff', 'Patient_Days']
    col_tag = [target_bed, target_top, target_tos, target_day]
    for index, name in enumerate(columns):
        counter = 0
        for i in range(1950, 1988):
            for index2, each in j.hos_development.iterrows():
                if each['Year_Founded'] < 1950 or each['Year_Founded'] == i:
                    if each[name] != -99:
                        counter += each[name]
            col_tag[index].append(counter)

    # Using the generated lists to create Province level dataframes that compile the aggregated values.
    year = pd.DataFrame(time, columns=["Year"])
    hos = pd.DataFrame(target_hos, columns=[f"{j.name} hos num"])
    bed = pd.DataFrame(target_bed, columns=[f"{j.name} hos bed"])
    top = pd.DataFrame(target_top, columns=[f"{j.name} hos top"])
    tos = pd.DataFrame(target_tos, columns=[f"{j.name} hos tos"])
    day = pd.DataFrame(target_day, columns=[f"{j.name} hos day"])

    hos_clean = pd.concat([year, hos, bed, top, tos, day], axis=1)

    # Subsequently, these dataframes are reassigned to their respective Province objects.
    j.hos_dev_clean = hos_clean

    #

    # Repeating the above process for the epidemiological center data for each province:
    target_epi = []
    target_bed = []
    target_top = []
    target_tos = []
    target_day = []

    # Resetting the dataframe index:
    j.epi_development = j.epi_development.reset_index()

    # Creating a cumulative column for the province's number of epidemiological centers:
    for i in range(1950, 1988):
        counter = 0
        for index, each in j.epi_development.iterrows():
            if each['Year_Founded'] <= i:
                counter += 1
        target_epi.append(counter)

    # Creating columns aggregating the province's cumulative number of: beds in epidemiological
    # centers, epidemiological center personnel, specialized epidemiological center staff,
    # and epidemiological center patient days.
    columns = ['Beds', 'Total_Personnel', 'Specialized_Staff', 'Patient_Days']
    col_tag = [target_bed, target_top, target_tos, target_day]
    for index, name in enumerate(columns):
        counter = 0
        for i in range(1950, 1988):
            for index2, each in j.epi_development.iterrows():
                if each['Year_Founded'] < 1950 or each['Year_Founded'] == i:
                    if each[name] != -99:
                        counter += each[name]
            col_tag[index].append(counter)

    # Using the generated lists to create Province level dataframes that compile the aggregated values.
    year = pd.DataFrame(time, columns=["Year"])
    epi = pd.DataFrame(target_epi, columns=[f"{j.name} epi num"])
    bed = pd.DataFrame(target_bed, columns=[f"{j.name} epi bed"])
    top = pd.DataFrame(target_top, columns=[f"{j.name} epi top"])
    tos = pd.DataFrame(target_tos, columns=[f"{j.name} epi tos"])
    day = pd.DataFrame(target_day, columns=[f"{j.name} epi day"])

    epi_clean = pd.concat([year, epi, bed, top, tos, day], axis=1)

    # Subsequently, these dataframes are reassigned to their respective Province objects.
    j.epi_dev_clean = epi_clean

#

# The next task is to loop through the "Province" objects to extract columns of a common
# measurement for final aggregation. I will do this in two loops, one for hospital data
# and the other for epidemiological data.

# Final aggregation of hospital data:
final_tables_hos = []
for index, m in enumerate(range(1, 6)):
    target_province_columns = [year]
    for n in province_list:
        target_column = n.hos_dev_clean.iloc[:, m]
        target_province_columns.append(target_column)
    table = pd.concat(target_province_columns, axis=1)
    final_tables_hos.append(table)

# Final aggregation of epidemiological center data:
final_tables_epi = []
for index, m in enumerate(range(1, 6)):
    target_province_columns = [year]
    for n in province_list:
        target_column = n.epi_dev_clean.iloc[:, m]
        target_province_columns.append(target_column)
    table = pd.concat(target_province_columns, axis=1)
    final_tables_epi.append(table)

#

# Output

# Writing tables to an SQL database:
# As far as I am aware, the following commented out code is correctly written to output the manipulated
# tables to a MySQL database. However, because of the scale of the project, I have faced the irreconcilable
# error "pymysql.err.OperationalError: (1117, 'Too many columns')"
# Which subsequently causes the error:
# "sqlalchemy.exc.OperationalError: (pymysql.err.OperationalError) (1117, 'Too many columns')"

# def insert_sqlalchemy_dataframe(user_id, pwd, host_name, db_name, df, table_name):
#     conn_str = f"mysql+pymysql://{user_id}:{pwd}@{host_name}/{db_name}"
#     sqlEngine = sqla.create_engine(conn_str, pool_recycle=3600)
#     connection = sqlEngine.connect()
#     df.to_sql(table_name, con=connection, if_exists='replace')  # , index_label='product_id');
#     connection.close()
#
hos_table_names = ["hos_num", "hos_beds", "hos_top", "hos_tos", "hos_day"]
epi_table_names = ["epi_num", "epi_beds", "epi_top", "epi_tos", "epi_day"]
# for index, object2 in enumerate(final_tables_hos):
#     insert_sqlalchemy_dataframe(user_id, pwd, host_name, db_name, object2, hos_table_names[index])
# for index, object2 in enumerate(final_tables_epi):
#     insert_sqlalchemy_dataframe(user_id, pwd, host_name, db_name, object2, epi_table_names[index])


# Writing final tables to local disk as .csv files:
for index2, object2 in enumerate(final_tables_hos):
    object2.to_csv(f'final_hos_{hos_table_names[index2]}.csv', index=False)
for index2, object2 in enumerate(final_tables_epi):
    object2.to_csv(f'final_epi_{epi_table_names[index2]}.csv', index=False)
