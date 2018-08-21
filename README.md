# fap
full analytics platform

projects
	+ collector: provide rest api to push data to data platform
	+ hadoop-worker: sync data from DataBus to hadoop cluster
	+ schema-manager: provide api/ui to view/add/ new schema.

## Deployment

### Clean up config before update

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