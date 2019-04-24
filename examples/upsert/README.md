# upsert

Select all rows from `old_db.persons` and insert them into `new_db.users`.

If a user already exists in `old_db.persons`, the user in `new_db.users` will be updated.

## Usage
Compile the executable:
```
go build -o migrate .
```

Run the executable:
```
./migrate -table=persons:users -from=user:pass@tcp(some-host:3306)/old_db?parseTime=true -to=user:pass@tcp(some-host:3306)/new_db?parseTime=true
```