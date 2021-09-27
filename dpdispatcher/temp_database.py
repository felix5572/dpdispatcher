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

create_job_table_sql = '''CREATE TABLE job_table (
    job_hash text,
    job_task_list json_array,
    resources json,
    job_state json,
    job_id text,
    fail_count integer
);'''

# create_index_sql = '''CREATE UNIQUE INDEX json_hash_index_unique
#     on task_table (hashvalue);'''

create_submission_table_sql = '''CREATE TABLE submission_table (
    submission_hash text,
    machine_dict json,
    work_base text,
    resources json,
    forward_common_files json,
    backward_common_files json,
    belonging_jobs_hash json
);'''


sql3 = '''CREATE TABLE test_table (
    submission_hash text,
    content json
);'''

cur = con.cursor()
cur.execute(create_job_table_sql)
cur.execute(create_submission_table_sql)
cur.execute(sql3)
con.commit()


#%%
insert_data_sql = """INSERT INTO job_table (job_hash, job_task_list, array) 
VALUES ('a8def712e', '{"job_id": 76887, "state":3}', '[1,2,3,4]');"""
cur.execute(insert_data_sql)
con.commit()

#%%%
insert_data_sql = """INSERT INTO test_table 
(submission_hash, content) VALUES ('a8def712e', '[1,2,3,4]'); """
cur.execute(insert_data_sql)
r = con.commit()

# %%
cur = con.cursor()
cur.execute('''SELECT * FROM job_table;''')
r = cur.fetchall()
print(r)
# %%
cur = con.cursor()
cur.execute('''SELECT json_extract(test_table.content, '$[1]') FROM test_table''')
r = cur.fetchall()
print(r)

# %%

a = {"a":1, "b":2, "c":3}

# %%
list(a.keys())
# %%
b = 'free_energy'

# %%
f"{b!r} is 300"
# %%
c = [1,2,3,4]
print(f"ad {c!r} bc")
# %%
