import re
import sys

#syntax: python dq_query_generator.py 'dq_rules_raw_string' 'interface_dq_bad_record_table_name'
#Arguments should be passed in 'single quotes'

#EXAMPLE: 
#python dq_query_generator.py '"dqrule_time_1":true,"dqrule_attach_state_1":true,"dqrule_current_cell_1":true,"dqrule_target2gcellid_1":true,"dqrule_imei_1":null,"dqrule_imei_2":false,"dqrule_imsi_1":true,"dqrule_imsi_2":true,"dqrule_mmeipaddress_1":true,"dqrule_msisdn_1":true,"dqrule_mobilecc_gn_1":true,"dqrule_mobileop_gn_1":true,"dqrule_s1ap_recordtype_1":true,"dqrule_enbipaddress_1":true' 's1ap_dq_bad_record'

dq_rules = sys.argv[1]
table_name = sys.argv[2]

select_list = []
as_list = []

rules_list=dq_rules.split(',')

for idx in range(len(rules_list)):
    rules_list[idx] = re.sub('(:true|:false|:null)$', '', rules_list[idx])

for idx in range(len(rules_list)):
    select_list.append('dq_rules[' + rules_list[idx] + ']')

for idx in range(len(rules_list)):
    rules_list[idx] = re.sub('dqrule_', '', rules_list[idx])
    rules_list[idx] = re.sub('"', '', rules_list[idx])
    as_list.append(select_list[idx] + ' AS ' + rules_list[idx])

as_list.append('count(*)')
select_command = ",".join(as_list)
group_by = ",".join(select_list)

print '################ FINAL QUERY ##################'
query = 'SELECT ' + select_command + ' FROM ' + table_name + ' GROUP BY ' + group_by + ';'
print query
