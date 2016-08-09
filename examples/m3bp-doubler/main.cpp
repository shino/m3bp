/*
 * This benchmark runs a flow graph that has following structure:
 *
 *      |I|---|I|---|I|-   -|I|
 *     /   \ /   \ /   \   /
 *  |G|     X     X     ...
 *     \   / \   / \   /   \
 *      |I|---|I|---|I|-   -|I|
 *     |--- depth * 2 nodes ---|
 *
 * G: outputs a buffer that has single <fragment_size>-bytes record
 * I: outputs copies of input buffer.
 */
#include <iostream>
#include <chrono>
#include <cstring>
#include "m3bp/m3bp.hpp"

class Generator : public m3bp::ProcessorBase {
private:
	size_t m_fragment_size;
public:
	Generator(size_t fragment_size)
		: m3bp::ProcessorBase(
			{ },
			{ m3bp::OutputPort("output").has_key(false) })
		, m_fragment_size(fragment_size)
	{ }
	virtual void global_initialize(m3bp::Task &) override {
		task_count(1);
	}
	virtual void run(m3bp::Task &task) override {
		auto writer = task.output(0);
		auto buffer = writer.allocate_buffer(m_fragment_size, 1);
		auto data = buffer.data_buffer();
		auto offsets = buffer.offset_table();
		memset(data, 0, m_fragment_size);
		offsets[0] = 0;
		offsets[1] = m_fragment_size;
		writer.flush_buffer(std::move(buffer), 1);
	}
};

class Identity : public m3bp::ProcessorBase {
public:
	Identity()
		: m3bp::ProcessorBase(
			{ m3bp::InputPort("input").movement(m3bp::Movement::ONE_TO_ONE) },
			{ m3bp::OutputPort("output").has_key(false) })
	{ }
	virtual void run(m3bp::Task &task) override {
		auto reader = task.input(0);
		auto writer = task.output(0);
		auto src = reader.raw_buffer();
		const auto src_data = src.key_buffer();
		const auto src_offsets = src.key_offset_table();
		const auto n = src.record_count();
		const auto m = src_offsets[n];
		auto dst = writer.allocate_buffer(m, n);
		memcpy(
			dst.offset_table(), src_offsets,
			sizeof(m3bp::size_type) * (n + 1));
		memcpy(dst.data_buffer(), src_data, m);
		writer.flush_buffer(std::move(dst), n);
	}
};

int main(int argc, const char *argv[]){
	if(argc < 3){
		std::cerr << "Usage: " << argv[0] << " fragment_size depth" << std::endl;
		return 0;
	}

	m3bp::Logger::add_destination_stream(std::clog, m3bp::LogLevel::INFO);

	const size_t fragment_size = atoi(argv[1]);
	const size_t depth = atoi(argv[2]);

	m3bp::FlowGraph graph;
	std::vector<m3bp::VertexDescriptor> vertices;
	vertices.emplace_back(graph.add_vertex("generator", Generator(fragment_size)));
	for(size_t i = 0; i < depth; ++i){
		std::vector<m3bp::VertexDescriptor> next;
		for(const auto &v : vertices){
			next.emplace_back(graph.add_vertex("identity", Identity()));
			graph.add_edge(v.output_port(0), next.back().input_port(0));
			next.emplace_back(graph.add_vertex("identity", Identity()));
			graph.add_edge(v.output_port(0), next.back().input_port(0));
		}
		vertices = std::move(next);
	}

	m3bp::Configuration config;
	m3bp::Context ctx;
	ctx.set_flow_graph(graph);
	ctx.set_configuration(config);

	const auto begin_time = std::chrono::steady_clock::now();
	ctx.execute();
	ctx.wait();
	const auto end_time = std::chrono::steady_clock::now();
	const auto duration = end_time - begin_time;
	const auto in_millisec =
		std::chrono::duration_cast<std::chrono::milliseconds>(duration);
	std::cout << "Execution time: " << in_millisec.count() << " [ms]" << std::endl;

	return 0;
}
