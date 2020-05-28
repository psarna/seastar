from dataclasses import dataclass, field
import argparse
import itertools
import json
import os
import subprocess
import yaml

@dataclass
class TestConfig:
	fs_type : str = ""
	fs_specific_params : dict = field(default_factory=dict)
	basic_params : dict = field(default_factory=dict)

@dataclass
class Results:
	median : float = 0.0
	mad : float = 0.0
	min : float = 0.0
	max : float = 0.0

@dataclass
class TestResults:
	config : TestConfig = TestConfig()
	results : Results = Results()

class TestRunner:
	def __init__(self, config_path, sfs_perf_path, kernel_fs_perf_path):
		self.sfs_perf_path = sfs_perf_path
		self.kernel_fs_perf_path = kernel_fs_perf_path
		with open(config_path, "r") as f:
			self.configs = yaml.load(f, Loader=yaml.SafeLoader)
		self._init_common_params()

	def _init_common_params(self):
		self.common_params = {}
		self.common_tester_params = {}
		if len(self.configs) > 0:
			config = self.configs[0]
			self.common_params["name"] = config["name"]
			self.common_params["runs-nb"] = config["runs-nb"]
			config_limits = config["operations-info"]["limits"]
			self.common_params["written-data-limit"] = config_limits.get("written-data-limit")
			self.common_params["read-data-limit"] = config_limits.get("read-data-limit")
			self.common_params["op-nb-limit"] = config_limits.get("op-nb-limit")
			self.common_params["device-path"] = config["device-path"]
			self.common_params["device-path"] = config["device-path"]
			self.common_tester_params["plot-param"] = config["plot-param"]

	def _iterate_configs(self):
		if len(self.configs) == 0:
			return
		config = self.configs[0]

		def copy(test_config):
			return TestConfig(test_config.fs_type, test_config.fs_specific_params.copy(), test_config.basic_params.copy())

		iter_basic_config = itertools.product(config["fs"],
			config["smp"],
			config["files-info"]["small-files-nb"],
			config["files-info"]["big-files-nb"],
			config["probabilities"]["small-prob"],
			config["probabilities"]["write-prob"],
			config["operations-info"]["seq-writes"],
			config["operations-info"]["parallelism"],
			config["operations-info"]["small-op-size-range"],
			config["operations-info"]["big-op-size-range"])
		for cur_basic_config in iter_basic_config:
			test_config = TestConfig()
			test_config.fs_type = cur_basic_config[0]
			test_config.basic_params["smp"] = cur_basic_config[1]
			test_config.basic_params["small-files-nb"] = cur_basic_config[2]
			test_config.basic_params["big-files-nb"] = cur_basic_config[3]
			test_config.basic_params["small-prob"] = cur_basic_config[4]
			test_config.basic_params["write-prob"] = cur_basic_config[5]
			test_config.basic_params["seq-writes"] = cur_basic_config[6]
			test_config.basic_params["parallelism"] = cur_basic_config[7]
			test_config.basic_params["small-op-size-range"] = cur_basic_config[8]
			test_config.basic_params["big-op-size-range"] = cur_basic_config[9]
			if test_config.fs_type == "SFS":
				iter_sfs_config = itertools.product(config["sfs-config"]["alignment"],
					config["sfs-config"]["cluster-size"],
					config["sfs-config"]["aligned-ops"])
				for cur_sfs_config in iter_sfs_config:
					test_config.fs_specific_params["alignment"] = cur_sfs_config[0]
					test_config.fs_specific_params["cluster-size"] = cur_sfs_config[1]
					test_config.fs_specific_params["aligned-ops"] = cur_sfs_config[2]
					yield copy(test_config)
			else:
				yield copy(test_config)

		self.configs.pop(0)
		self._init_common_params()

	def _execute_test(self, test_config):
		cmd = []
		def add_cmd_option(key, val):
			if val != None:
				if type(val) == list and len(val) == 2:
					cmd.append(f"--{key}={val[0]},{val[1]}")
				else:
					cmd.append(f"--{key}={val}")

		json_output = "/tmp/fs_perf_results.json"

		if test_config.fs_type == "SFS":
			cmd.append(self.sfs_perf_path)
		else:
			cmd.append(self.kernel_fs_perf_path)
			add_cmd_option("fs-type", test_config.fs_type)
		add_cmd_option("blocked-reactor-reports-per-minute", 100000)
		add_cmd_option("blocked-reactor-notify-ms", 10)
		add_cmd_option("json-output", json_output)
		cmd.append("--no-stdout")

		for (param_key, param_val) in test_config.fs_specific_params.items():
			add_cmd_option(param_key, param_val)

		for (param_key, param_val) in test_config.basic_params.items():
			add_cmd_option(param_key, param_val)

		for (param_key, param_val) in self.common_params.items():
			add_cmd_option(param_key, param_val)

		ret_code = subprocess.call(cmd)
		if ret_code:
			raise RuntimeError(f"Error occurred while executing the folowing test config: {test_config}, return code: {ret_code}")

		with open(json_output, "r") as out_file:
			result_json = json.load(out_file)

		test_res_map = result_json["results"]["example"]
		res = Results(float(test_res_map["median"]), float(test_res_map["mad"]), float(test_res_map["min"]), float(test_res_map["max"]))
		return TestResults(test_config, res)

	def get_configs_left(self):
		return len(self.configs)

	def get_common_tester_params(self):
		return self.common_tester_params

	def get_common_params(self):
		return self.common_params

	def start_tests(self):
		for test_config in self._iterate_configs():
			yield self._execute_test(test_config)

@dataclass
class LineInfo:
	info : dict
	xs : list
	ys : list

def prepare_lines(test_results, plot_x_axis_aggregator):
	if len(test_results) == 0:
		return []

	@dataclass
	class TestInfo:
		info : dict
		results : Results
	tests_info = [TestInfo(dict({"fs": test_result.config.fs_type},
		**test_result.config.basic_params,
		**test_result.config.fs_specific_params), test_result.results) for test_result in test_results]

	@dataclass
	class PointInfo:
		info : list
		x : float
		y : float
	points = [PointInfo([], test_info.info[plot_x_axis_aggregator], 0.0) for test_info in tests_info]

	common = []
	for key in tests_info[0].info.keys():
		if key == plot_x_axis_aggregator or all([tests_info[0].info[key] == test_info.info.get(key) for test_info in tests_info]):
			common.append(key)

	for key in common:
		for test_info in tests_info:
			del test_info.info[key]

	for test_info, point in zip(tests_info, points):
		point.y = test_info.results.median
		point.info = list(test_info.info.items())

	points = sorted(points, key=lambda point: point.info)

	lines = []
	prev_point_info = None
	for point in points:
		if point.info == prev_point_info:
			lines[-1].xs.append(point.x)
			lines[-1].ys.append(point.y)
		else:
			lines.append(LineInfo(point.info, [point.x], [point.y]))
			prev_point_info = point.info

	return lines

def write_plot_info(plot_name, x_axis_name, y_axis_name, lines, f):
	line_dicts = []
	for line in lines:
		line_dict = {}
		line_dict["name"] = str(line.info)
		line_dict["xs"] = line.xs
		line_dict["ys"] = line.ys
		line_dicts.append(line_dict)
	plot_dict = {}
	plot_dict["lines-info"] = line_dicts
	plot_dict["plot-name"] = plot_name
	plot_dict["x-axis"] = x_axis_name
	plot_dict["y-axis"] = y_axis_name
	yaml.dump(plot_dict, f)

def write_tmp_result(test_result, f):
	out_dict = {}
	out_dict["config"] = {}
	out_dict["config"]["fs-type"] = test_result.config.fs_type
	if len(test_result.config.fs_specific_params) > 0:
		out_dict["config"]["fs-specific-params"] = test_result.config.fs_specific_params
	out_dict["config"]["basic-params"] = test_result.config.basic_params
	out_dict["results"] = {}
	out_dict["results"]["median"] = test_result.results.median
	out_dict["results"]["mad"] = test_result.results.mad
	out_dict["results"]["min"] = test_result.results.min
	out_dict["results"]["max"] = test_result.results.max
	yaml.dump([out_dict], f)

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument("config", help="Path to YAML config file", type=str)
	parser.add_argument("sfs_perf", help="Path to sfs_perf", type=str)
	parser.add_argument("kernel_fs_perf", help="Path to kernel_fs_perf", type=str)
	args = parser.parse_args()
	runner = TestRunner(args.config, args.sfs_perf, args.kernel_fs_perf)
	while runner.get_configs_left() > 0:
		test_results = []
		plot_x_axis_aggregator = runner.get_common_tester_params()["plot-param"]
		common_params = runner.get_common_params()
		os.makedirs("test_results", exist_ok=True)
		with open(f"test_results/{common_params['name']}_tmp_results.yaml", "w") as out:
			for test_result in runner.start_tests():
				test_results.append(test_result)
				write_tmp_result(test_result, out)
		lines = prepare_lines(test_results, plot_x_axis_aggregator)
		for line in lines:
			print(line)
		with open(f"test_results/{common_params['name']}.yaml", "w") as out:
			write_plot_info(common_params['name'], plot_x_axis_aggregator,
				"time", lines, out)
