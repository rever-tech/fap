# fap
full analytics platform

projects
	+ collector: provide rest api to push data to data platform
	+ hadoop-worker: sync data from DataBus to hadoop cluster
	+ schema-manager: provide api/ui to view/add/ new schema.

## Deployment

Requirements:

- docker 1.13.0+
- docker-compose 1.10.0+
- maven 3

### Clean up project config before update

```bash
./cleanup_conf.sh
```

### Build scala projects

```bash
./build-projects.sh
```

### Build docker images

```bash
docker-compose build
```

### Start all services

```bash
docker-compose up -d
```

Collector API working at port: 10111

### Clean up

```bash
docker-compose down
```