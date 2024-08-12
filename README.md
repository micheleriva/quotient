# Quotient

[![Go](https://github.com/micheleriva/quotient/actions/workflows/go.yml/badge.svg)](https://github.com/micheleriva/quotient/actions/workflows/go.yml)

A massively scalable, fast, distributed quotient filter.

# Status

- [x] Quotient Filter implementation 
- [x] Web server
- [ ] Expiring keys
- [ ] RAFT distribution
- [ ] Docker image

# APIs

Quotient has two simple APIs:

### Set key

Example request:
```sh
curl -X POST http://localhost:9000/v1/insert \
  -d '{ "key": "b4912a59-b0ed-4f68-9042-0651c28c3e31" }'
  -H 'content-type: application/json'
```

Example response:
```json
{
  "key": "b4912a59-b0ed-4f68-9042-0651c28c3e31",
  "status": "inserted"
}
```

### Check if a key exists

Example request:
```sh
curl http://localhost:9000/v1/exists?key=b4912a59-b0ed-4f68-9042-0651c28c3e31
```

Example response:
```json
{
  "key": "b4912a59-b0ed-4f68-9042-0651c28c3e31",
  "exists": true,
  "elapsed": 4167
}
```

### Remove a key

Example request:
```sh
curl -X POST http://localhost:9000/v1/remove \
  -d '{ "key": "b4912a59-b0ed-4f68-9042-0651c28c3e31" }'
  -H 'content-type: application/json'
```

Example response:
```json
{
  "key": "b4912a59-b0ed-4f68-9042-0651c28c3e31",
  "status": "removed"
}
```

### Count the number of keys stored

Example request:
```sh
curl http://localhost:9000/v1/count
```

Example response:
```json
{
  "count": 1
}
```

# Why Golang

Even though I'm not a Googler (nor a researcher), I'm fairly young and I learned Python and JavaScript.

I'm not capable of understanding a brilliant language but I want to build good software. Golang is easy for me to understand and to adopt.

Thank you, Rob!

![I'm dumb](/misc/imdumb.jpg)