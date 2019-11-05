DXDA_ROOT=/go/src/github.com/dnanexus/dxda

all : dxfuse copy_all manifest
	dx build dxda_correctness -f --destination dxfuse_test_data:/applets/dxda_correctness

dxfuse :
	go build -o dx-download-agent ${DXDA_ROOT}/cmd/dx-download-agent/dx-download-agent.go

copy_all : dxfuse
	mkdir -p dxda_correctness/resources/usr/bin
	cp -f dx-download-agent dxda_correctness/resources/usr/bin/

manifest:
	dx select dxfuse_test_data
	rm -f manifest.json.bz2
	python ${DXDA_ROOT}/scripts/create_manifest.py -r /correctness
	dx rm -f manifest.json.bz2 || true
	dx upload manifest.json.bz2

exec :
	dx run applets/dxda_correctness --instance-type=mem1_ssd1_x4 -imanifest=dxfuse_test_data:/manifest.json.bz2 -y --watch
