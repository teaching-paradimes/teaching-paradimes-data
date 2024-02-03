# teaching-paradimes-data
teaching paradimes data processing repo

## set up local baserow

```shell
docker run \
  -d \
  --name baserow \
  -e BASEROW_PUBLIC_URL=http://localhost \
  -v baserow_data:/baserow/data \
  -p 80:80 \
  -p 443:443 \
  --restart unless-stopped \
  baserow/baserow:1.22.2
```

## prepare date

* run `prepare_data.py`
* run `import2baserow.py`
