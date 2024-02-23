- step 1: open terminal from your terminal
- step 2: go to the `dbt` folder and wrtie the following command in terminal
```cmd
dbt init
```
- step 3: enter a name for you project (as dbt asks you a name)
- step 4: choose a database you want to use (as dbt asks you for it)
- step 5: provide all the data required for dbt to connect to your database
- step6: now you have a project in dbt folder, now create a `profiles.yml` in the roject folder with the schema that `profiles.yml` has in the `dbt` folder
```yml
# https://docs.getdbt.com/docs/core/connect-data-platform/profiles.yml

base:
  outputs:

    dev:
      type: duckdb

  target: dev

```
- step7: go to the mage-ai dashboard (localhost:6789), create a pipeline and choose a dbt block with the name of `All Models`.
- step8: choose your dbt probect from the top-left of the dbt block. then write the command you want to execute in dbt (the deafult is `dbt run`). run the dbt block.
> have in mind that all the block you create will be saved in another folder named `dbts`