# Campaign Finance Check

Search FEC individual contribution records by employer, zip code, or candidate name.

Every individual contribution over $200 to federal candidates, parties, and PACs is public record.
This tool makes that data searchable.

## Search Modes

- **By Employer** — See where employees of a company direct their political donations
- **By Zip Code** — See the top political donors in any zip code
- **By Candidate** — See who's funding a candidate and from which industries

## Data Source

All data is from [FEC Bulk Downloads](https://www.fec.gov/data/browse-data/?tab=bulk-data) (2023-2024 election cycle).

## Pipeline

The `pipeline/build.py` script downloads FEC bulk data, joins contribution records
to committee and candidate master files, and pre-aggregates into static JSON files
for client-side search.

```bash
# Download FEC bulk data and build the site
cd pipeline
curl -L 'https://www.fec.gov/files/bulk-downloads/2024/indiv24.zip' -o indiv24.zip
curl -L 'https://www.fec.gov/files/bulk-downloads/2024/cm24.zip' -o cm24.zip
curl -L 'https://www.fec.gov/files/bulk-downloads/2024/cn24.zip' -o cn24.zip
curl -L 'https://www.fec.gov/files/bulk-downloads/2024/ccl24.zip' -o ccl24.zip
unzip cm24.zip && unzip cn24.zip && unzip ccl24.zip
python3 build.py
```

## Privacy

This is a static lookup tool. No data is collected, no searches are tracked, no cookies are set.
All data displayed is from publicly available FEC records.

## License

MIT
