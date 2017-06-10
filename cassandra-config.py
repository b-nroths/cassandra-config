# python cli.py --ycsb_path /scripts/ycsb-0.12.0 --yaml_path /etc/cassandra/cassandra.yaml --ycsb_workload b
import subprocess, sys
import yaml
from cassandra_yaml import document
import os
import time
import click
import math

paramters = {
	'concurrent_reads': {
		'min': 1,
		'max': 12,
		'scale': 'log',
		'vals_to_test': [32, 512],
		'default': 32,
		'optimal_value': None
	},
	'concurrent_writes': {
		'min': 1,
		'max': 12,
		'scale': 'log',
		'default': 32, 
		'vals_to_test': [32, 512],
		'optimal_value': None
	},
	'read_request_timeout_in_ms': {
		'min': 250,
		'max': 5000,
		'scale': 'linear',
		'default': 5000,
		'vals_to_test': [1833, 3416],
		'optimal_value': None
	},
	'compaction_throughput_mb_per_sec': {
		'min': 4,
		'max': 32,
		'scale': 'linear',
		'default': 16,
		'vals_to_test': [10, 26],
		'optimal_value': None
	},
	'concurrent_counter_writes': {
		'min': 1,
		'max': 12,
		'scale': 'log',
		'default': 32,
		'vals_to_test': [32, 512],
		'optimal_value': None
	},
}

@click.command()
@click.option('--ycsb_path', prompt='Enter the path to ycsb', help='Enter the path to ycsb, enter absolute path')
@click.option('--yaml_path', prompt='Enter the path to your cassandra.yaml file', help='Enter the path to your cassandra.yaml file, enter absolute path')
@click.option('--ycsb_workload', prompt='Enter the ycsb workload', help='a (50r/50w), b (95r/5w), c (100r/0w)')

def hello(ycsb_path, yaml_path, ycsb_workload):
	"""Simple program optimizes your cassandra.yaml configuration."""
	# "/scripts//etc/cassandra/bin/ycsb"
	print ycsb_path, yaml_path, ycsb_workload
	with open(yaml_path, 'r') as stream:
		yaml_file = yaml.load(stream)

	yaml_file['concurrent_reads'] 					= paramters['concurrent_reads']['default']
	yaml_file['concurrent_writes'] 					= paramters['concurrent_writes']['default']
	yaml_file['read_request_timeout_in_ms'] 		= paramters['read_request_timeout_in_ms']['default']
	yaml_file['compaction_throughput_mb_per_sec'] 	= paramters['compaction_throughput_mb_per_sec']['default']
	yaml_file['concurrent_counter_writes'] 			= paramters['concurrent_counter_writes']['default']


	with open(yaml_path, 'w') as outfile:
		yaml.dump(yaml_file, outfile, default_flow_style=False)
	
	## restart cassandra
	FNULL = open(os.devnull, 'w')
	command = ['/usr/sbin/service', 'cassandra', 'restart'];
	subprocess.call(command, shell=False, stdout=FNULL, stderr=FNULL)

	## how many runs to do
	p = subprocess.call("mkdir param_test/default", shell=True)
	for i in range(10):
		# print i, file_name, "param_test/%s/%s-stdout.txt" % (file_name, i)
		with open("param_test/default/%s-stdout.txt" % i, "wb") as out, open("param_test/default/%s-stderr.txt" % i, "wb") as err:
			p = subprocess.Popen("./ycsb-0.12.0/bin/ycsb run basic -P ycsb-0.12.0/workloads/workload%s" % ycsb_workload, 
				shell=True, 
				stdout=out,
				stderr=err)
			p.communicate()

			## read test results > data structure
	res = {}
	for folder in os.listdir("param_test/"):
		for file in os.listdir("param_test/default"):
			if file.endswith("out.txt"):
				with open("param_test/default/%s" % file, 'r') as f:
					for line in f.readlines():
						# print line
						if "[READ]" in line:
							l 	= line.split(', ')
							key = l[1]
							val = int(l[2].replace("\n", "").split(".")[0])
							if key not in res:
								res[key] = {}
							if folder in res[key]:
								res[key][folder].append(val)
							else:
								res[key][folder] = [val]

	print "default performance", 1.0 * sum(res['AverageLatency(us)']['default'])/len(res['AverageLatency(us)']['default'])

	iteration = 1
	while iteration < 4:
		# ## itereate through combinations and run results
		params = []
		param_names = []
		p = subprocess.call("rm -Rf param_test", shell=True)
		p = subprocess.call("mkdir param_test", shell=True)
		for a_val in paramters['concurrent_reads']['vals_to_test']:
			for b_val in paramters['concurrent_writes']['vals_to_test']:
				for c_val in paramters['read_request_timeout_in_ms']['vals_to_test']:
					for d_val in paramters['compaction_throughput_mb_per_sec']['vals_to_test']:
						for e_val in paramters['concurrent_counter_writes']['vals_to_test']:
							yaml_file['concurrent_reads'] 					= a_val
							yaml_file['concurrent_writes'] 					= b_val
							yaml_file['read_request_timeout_in_ms'] 		= c_val
							yaml_file['compaction_throughput_mb_per_sec'] 	= d_val
							yaml_file['concurrent_counter_writes'] 			= e_val
							
							## save params
							with open(yaml_path, 'w') as outfile:
								yaml.dump(yaml_file, outfile, default_flow_style=False)
							
							## restart cassandra
							FNULL = open(os.devnull, 'w')
							command = ['/usr/sbin/service', 'cassandra', 'restart'];
							subprocess.call(command, shell=False, stdout=FNULL, stderr=FNULL)

							file_name = "%s-%s-%s-%s-%s" % (a_val, b_val, c_val, d_val, e_val)
							print "\t%s", file_name
							
							## how many runs to do
							p = subprocess.call("mkdir param_test/%s" % file_name, shell=True)
							for i in range(10):
								# print i, file_name, "param_test/%s/%s-stdout.txt" % (file_name, i)
								with open("param_test/%s/%s-stdout.txt" % (file_name, i), "wb") as out, open("param_test/%s/%s-stderr.txt" % (file_name, i), "wb") as err:
									p = subprocess.Popen("./ycsb-0.12.0/bin/ycsb run basic -P ycsb-0.12.0/workloads/workload%s" % ycsb_workload, 
										shell=True, 
										stdout=out,
										stderr=err)
									p.communicate()

		## read test results > data structure
		res = {}
		for folder in os.listdir("param_test/"):
			for file in os.listdir("param_test/%s" % folder):
				if file.endswith("out.txt"):
					with open("param_test/%s/%s" % (folder, file), 'r') as f:
						for line in f.readlines():
							# print line
							if "[READ]" in line:
								l 	= line.split(', ')
								key = l[1]
								val = int(l[2].replace("\n", "").split(".")[0])
								if key not in res:
									res[key] = {}
								if folder in res[key]:
									res[key][folder].append(val)
								else:
									res[key][folder] = [val]

		## find optimal values
		min_val = 999999
		for read_val in res['AverageLatency(us)']:
			# print read_val
			avg = sum(res['AverageLatency(us)'][read_val])/len(res['AverageLatency(us)'][read_val])
			# print read_val, avg
			if avg <= min_val:
				param = read_val
				min_val = avg
		print "OPTIMAL", param, min_val
		# exit(0)
		concurrent_reads, concurrent_writes, read_request_timeout_in_ms, compaction_throughput_mb_per_sec, concurrent_counter_writes = param.split('-')
		# set optimal values
		paramters['concurrent_reads']['optimal_value'] 					= int(concurrent_reads)
		paramters['concurrent_writes']['optimal_value'] 				= int(concurrent_writes)
		paramters['read_request_timeout_in_ms']['optimal_value'] 		= int(read_request_timeout_in_ms)
		paramters['compaction_throughput_mb_per_sec']['optimal_value'] 	= int(compaction_throughput_mb_per_sec)
		paramters['concurrent_counter_writes']['optimal_value'] 		= int(concurrent_counter_writes)
		# update values to test
		for param in paramters:
			optimal = paramters[param]['optimal_value']
			# print "\n"
			# print param, optimal, paramters[param]['min'], paramters[param]['max'], paramters[param]['vals_to_test']
			params_to_test = []
			if paramters[param]['scale'] == 'log':
				# print param
				# print "a", paramters[param]['vals_to_test'][1], paramters[param]['vals_to_test'][0]
				diff = math.log(paramters[param]['vals_to_test'][1], 2) - math.log(paramters[param]['vals_to_test'][0], 2)
				diff = int(round(diff/2, 0))
				# print diff
				min_val = int(round(math.log(optimal, 2) - diff, 0))
				max_val = int(round(math.log(optimal, 2) + diff, 0))
				# print min_val, max_val, optimal, diff
				if min_val >= paramters[param]['min']:
					params_to_test.append(2**min_val)
				if max_val <= paramters[param]['max']:
					params_to_test.append(2**max_val)
				params_to_test.append(optimal)
			else:
				diff = paramters[param]['vals_to_test'][1] - paramters[param]['vals_to_test'][0]
				# print "a", paramters[param]['vals_to_test'][1], paramters[param]['vals_to_test'][0]
				diff = int(round(diff/2, 0))
				min_val = optimal - diff
				max_val = optimal + diff
				if min_val >= paramters[param]['min']:
					params_to_test.append(min_val)
				if max_val <= paramters[param]['max']:
					params_to_test.append(max_val)
				params_to_test.append(optimal)

			paramters[param]['vals_to_test'] = sorted(list(set(params_to_test)))
			print param, paramters[param]['optimal_value'], paramters[param]['vals_to_test']
			# print paramters[param]['vals_to_test']
		iteration += 1
		
		print "\n"
		# exit(0)


if __name__ == '__main__':
    hello()