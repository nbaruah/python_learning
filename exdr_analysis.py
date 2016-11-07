import sys

columns=sys.argv[1]
table_name = sys.argv[2]

select_list = []
col_list=columns.split(',')

for idx in range(len(col_list)):
    select_list.append("select count(*)-1, '" + col_list[idx] + "' from " + table_name + " where  `" + col_list[idx] + "` is null")

union_all = " union all ".join(select_list)

print '################ FINAL QUERY ##################'
query = union_all + ';'
print query
