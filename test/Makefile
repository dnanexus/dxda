DXDA_ROOT=/go/src/github.com/dnanexus/dxda

all : dxda copy_all correctness benchmark
	dx build dxda_benchmark -f --destination dxfuse_test_data:/applets/dxda_benchmark

dxda :
	go build -o /go/bin/dx-download-agent ${DXDA_ROOT}/cmd/dx-download-agent/dx-download-agent.go

copy_all : dxda
	mkdir -p dxda_correctness/resources/usr/bin
	cp -f /go/bin/dx-download-agent dxda_correctness/resources/usr/bin/
	mkdir -p dxda_benchmark/resources/usr/bin
	cp -f /go/bin/dx-download-agent dxda_benchmark/resources/usr/bin/

correctness:
	dx select dxfuse_test_data
	rm -f manifest.json.bz2
	python ${DXDA_ROOT}/scripts/create_manifest.py -r /correctness
	mv manifest.json.bz2 manifest_correctness.json.bz2
	dx rm -f manifest_correctness.json.bz2 || true
	dx upload manifest_correctness.json.bz2
	dx build dxda_correctness -f --destination dxfuse_test_data:/applets/dxda_correctness
	rm -f manifest_correctness.json.bz2

# get the manifest from "Reference Data"
benchmark:
	dx select "Reference\ Data"
	rm -f manifest.json.bz2
	python ${DXDA_ROOT}/scripts/create_manifest.py -r :/pVCF
	mv manifest.json.bz2 manifest_benchmark.json.bz2
	dx select dxfuse_test_data
	dx rm -f manifest_benchmark.json.bz2 || true
	dx upload manifest_benchmark.json.bz2
	dx build dxda_benchmark -f --destination dxfuse_test_data:/applets/dxda_benchmark
	rm -f manifest_benchmark.json.bz2

exec :
#	dx run applets/dxda_correctness --instance-type=mem1_ssd1_x4 -imanifest=dxfuse_test_data:/manifest.json.bz2 -y --watch
	dx run dxfuse_test_data:/applets/dxda_benchmark --instance-type=mem1_ssd1_v2_x8 -imanifest=dxfuse_test_data:/manifest_benchmark.json.bz2 -y --watch
