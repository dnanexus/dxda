DXDA_ROOT=..

all : prep_dx_env correctness benchmark

dxda :
	go build -o ${DXDA_ROOT}/dx-download-agent ${DXDA_ROOT}/cmd/dx-download-agent/dx-download-agent.go

prep_dx_env:
	dx select dxfuse_test_data
	dx mkdir -p /applets

correctness: dxda
	mkdir -p dxda_correctness/resources/usr/bin
	cp -f ${DXDA_ROOT}/dx-download-agent dxda_correctness/resources/usr/bin/
	dx build dxda_correctness -f --destination dxfuse_test_data:/applets/dxda_correctness

# get the manifest from "Reference Data"
benchmark: dxda
	mkdir -p dxda_benchmark/resources/usr/bin
	cp -f ${DXDA_ROOT}/dx-download-agent dxda_benchmark/resources/usr/bin/
	dx build dxda_benchmark -f --destination dxfuse_test_data:/applets/dxda_benchmark

manifests: correctness_manifest

correctness_manifest:
	rm -f manifest.json.bz2
	python3 ${DXDA_ROOT}/scripts/create_manifest.py -r /correctness
	mv manifest.json.bz2 correctness.manifest.json.bz2
	dx rm -f correctness.manifest.json.bz2 || true
	dx upload correctness.manifest.json.bz2
	rm -f correctness.manifest.json.bz2

benchmark_manifest:
	dx select "Reference\ Data"
	rm -f manifest.json.bz2
	python3 ${DXDA_ROOT}/scripts/create_manifest.py -r :/pVCF
	mv manifest.json.bz2 benchmark.manifest.json.bz2
	dx select dxfuse_test_data
	dx rm -f benchmark.manifest.json.bz2 || true
	dx upload benchmark.manifest.json.bz2
	rm -f benchmark.manifest.json.bz2

exec :
#	dx run applets/dxda_correctness --instance-type=mem1_ssd1_x4 -imanifest=dxfuse_test_data:/manifest.json.bz2 -y --watch
	dx run dxfuse_test_data:/applets/dxda_benchmark --instance-type=mem1_ssd1_v2_x8 -imanifest=dxfuse_test_data:/benchmark.manifest.json.bz2 -y --watch
