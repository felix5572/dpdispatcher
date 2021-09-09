#%%
import sqlite3
con  = sqlite3.connect('example.db')
cur = con.cursor()

#%%
# Create table
cur.execute('''CREATE TABLE Task
                (command text, task_work_path text, forward_files text, qty real, price real)''')

# Insert a row of data
cur.execute("INSERT INTO stocks VALUES ('2006-01-05','BUY','RHAT',100,35.14)")

# Save (commit) the changes
con.commit()

# We can also close the connection if we are done with it.
# Just be sure any changes have been committed or they will be lost.
con.close()

# %%

create_table_sql = '''CREATE TABLE task_table (
    hashvalue text,
    content json
);'''

create_index_sql = '''CREATE UNIQUE INDEX json_hash_index_unique
    on task_table (hashvalue);'''
cur = con.cursor()
cur.execute(create_table_sql)
cur.execute(create_index_sql)
con.commit()
# con.close()

#%%
insert_data_sql = """INSERT INTO task_table (hashvalue, content) VALUES ('a8def712e', '{"job_id": 76887, "state":3}')"""
cur.execute(insert_data_sql)
con.commit()

# %%
cur = con.cursor()
cur.execute('''SELECT * FROM task_table''')
r = cur.fetchall()
print(r)
# %%
cur = con.cursor()
cur.execute('''SELECT * FROM task_table''')
r = cur.fetchall()
print(r)
