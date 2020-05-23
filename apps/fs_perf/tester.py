from dataclasses import dataclass
import argparse
import itertools
import json
import os
import subprocess
import yaml

@dataclass
class TestConfig:
	fs : str = ""
	device_path : str = ""
	runs_nb : int = 0
	shards_nb : int = 0
	alignment : int = 0
	cluster_size : int = None
	small_files_nb : int = 0
	big_files_nb : int = 0
	small_prob : float = 0.0
	write_prob : float = 0.0
	written_data_limit : int = None
	read_data_limit : int = None
	op_nb_limit : int = None
	aligned_ops : bool = False
	seq_writes : bool = False
	small_op_size_range : (int, int) = (0, 0)
	big_op_size_range : (int, int) = (0, 0)
	name : str = ""

@dataclass
class Result:
	median : float = 0.0
	mad : float = 0.0
	min : float = 0.0
	max : float = 0.0

@dataclass
class TestResult:
	config : TestConfig = TestConfig()
	results : Result = Result()

class TestRunner:
	def __init__(self, config_path, sfs_perf_path, kernel_fs_perf_path):
		self.sfs_perf_path = sfs_perf_path
		self.kernel_fs_perf_path = kernel_fs_perf_path
		with open(config_path, "r") as f:
			self.configs = yaml.load(f, Loader=yaml.SafeLoader)

	def _iterate_configs(self):
		for config in self.configs:
			test_config = TestConfig()
			test_config.name = config["name"]
			test_config.runs_nb = config["runs_nb"]
			test_config.shards_nb = config["shards_nb"]
			config_limits = config["operations_info"]["limits"]
			test_config.written_data_limit = config_limits.get("written_data_limit")
			test_config.read_data_limit = config_limits.get("read_data_limit")
			test_config.op_nb_limit = config_limits.get("op_nb_limit")
			test_config.device_path = config["device_path"]

			for fs in config["fs"]:
				test_config.fs = fs
				if fs == "SFS":
					iter_config = itertools.product(config["shards_nb"],
						config["files_info"]["small_files_nb"],
						config["files_info"]["big_files_nb"],
						config["probabilities"]["small_prob"],
						config["probabilities"]["write_prob"],
						config["operations_info"]["seq_writes"],
						config["operations_info"]["small_op_size_range"],
						config["operations_info"]["big_op_size_range"],
						config["sfs_config"]["alignment"],
						config["sfs_config"]["cluster_size"],
						config["sfs_config"]["aligned_ops"])
				else: # TODO: reduce copy pasting
					iter_config = itertools.product(config["shards_nb"],
						config["files_info"]["small_files_nb"],
						config["files_info"]["big_files_nb"],
						config["probabilities"]["small_prob"],
						config["probabilities"]["write_prob"],
						config["operations_info"]["seq_writes"],
						config["operations_info"]["small_op_size_range"],
						config["operations_info"]["big_op_size_range"],
						[None],
						[None],
						[None])
				for cur_config in iter_config:
					test_config.shards_nb = cur_config[0]
					test_config.small_files_nb = cur_config[1]
					test_config.big_files_nb = cur_config[2]
					test_config.small_prob = cur_config[3]
					test_config.write_prob = cur_config[4]
					test_config.seq_writes = cur_config[5]
					test_config.small_op_size_range = cur_config[6]
					test_config.big_op_size_range = cur_config[7]
					test_config.alignment = cur_config[8]
					test_config.cluster_size = cur_config[9]
					test_config.aligned_ops = cur_config[10]
					yield test_config

	def _execute_test(self, test_config):
		cmd = []
		def add_cmd_option(key, val):
			if val != None:
				cmd.append(f"--{key}={val}")
		def add_cmd_option_pair(key, val):
			if val != None:
				cmd.append(f"--{key}={val[0]},{val[1]}")

		json_output = "/tmp/fs_perf_results.json"

		if test_config.fs == "SFS":
			cmd.append(self.sfs_perf_path)
			add_cmd_option("cluster-size", test_config.cluster_size)
			add_cmd_option("aligned", test_config.aligned_ops)
			add_cmd_option("alignment", test_config.alignment)
		else:
			cmd.append(self.kernel_fs_perf_path)
			add_cmd_option("fs-type", test_config.fs)

		add_cmd_option("blocked-reactor-reports-per-minute", 0)
		add_cmd_option("device-path", test_config.device_path)
		add_cmd_option("smp", test_config.shards_nb)
		add_cmd_option("small-files-nb", test_config.small_files_nb)
		add_cmd_option("big-files-nb", test_config.big_files_nb)
		add_cmd_option("small-prob", test_config.small_prob)
		add_cmd_option("write-prob", test_config.write_prob)
		add_cmd_option("runs-nb", test_config.runs_nb)
		add_cmd_option("op-nb-limit", test_config.op_nb_limit)
		add_cmd_option("written-data-limit", test_config.written_data_limit)
		add_cmd_option("read-data-limit", test_config.read_data_limit)
		add_cmd_option("name", test_config.name)
		add_cmd_option("seq-writes", test_config.seq_writes)
		add_cmd_option_pair("small-op-size-range", test_config.small_op_size_range)
		add_cmd_option_pair("big-op-size-range", test_config.big_op_size_range)
		add_cmd_option("json-output", json_output)
		cmd.append("--no-stdout")

		ret_code = subprocess.call(cmd)
		if ret_code:
			raise RuntimeError(f"Error occurred while executing the folowing test config: {test_config}, return code: {ret_code}")

		with open(json_output, "r") as out_file:
			result_json = json.load(out_file)

		test_res_map = result_json["results"]["example"]
		res = Result(float(test_res_map["median"]), float(test_res_map["mad"]), float(test_res_map["min"]), float(test_res_map["max"]))
		return TestResult(test_config, res)


	def start_tests(self):
		for test_config in self._iterate_configs():
			yield self._execute_test(test_config)

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument("config", help="Path to YAML config file", type=str)
	parser.add_argument("sfs_perf", help="Path to sfs_perf", type=str)
	parser.add_argument("kernel_fs_perf", help="Path to kernel_fs_perf", type=str)
	args = parser.parse_args()
	runner = TestRunner(args.config, args.sfs_perf, args.kernel_fs_perf)
	for test_result in runner.start_tests():
		print(test_result.results)
