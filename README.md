# teaching-paradimes-data
teaching paradimes data processing repo, converts MS-Access database into a baserow project  

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

## prepare and import data to baserow

* run `prepare_data.py`
* run `migrate.py`
* run `import.py`


ideally needed only run to be once (which happend now 2024-02-16, see [baserow-table](https://baserow.acdh-dev.oeaw.ac.at/database/520/table/3037/10954))