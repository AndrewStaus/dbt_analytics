def model(dbt, session):
    session.use_schema("mrt_marketing")
    dbt.config(
        python_version="3.11",
        packages=["faker"],
        materialization="incremental",
        incremental_strategy="merge",
        unique_key="account_id"
    )

    from snowflake.snowpark import functions as F
    from faker import Faker

    fake = Faker()

    @F.udf
    def syn_first_name(col: str) -> str:
        fake.seed_locale('en_US', col)
        return fake.first_name()
    
    @F.udf
    def syn_last_name(col: str) -> str:
        fake.seed_locale('en_US', col)
        return fake.last_name()


    df = dbt.ref("stg_accounts_db__accounts")
    cols = df.columns

    if dbt.is_incremental:
            max_from_this = f"select max(loaded_at) from {dbt.this}"
            df = df.filter(df["loaded_at"] >= session.sql(max_from_this).collect()[0][0])

    df_fn = (
        df
        .select("first_name")
        .distinct()
        .with_column("syn_first_name",
                     F.lower(syn_first_name(F.col("first_name"))))
    )
    
    df_ln = (
        df
        .select("last_name")
        .distinct()
        .with_column("syn_last_name",
                     F.lower(syn_last_name(F.col("last_name"))))
    )

    df = (
        df
        .join(df_fn, "first_name")
        .join(df_ln, "last_name")
        .with_column("first_name", df_fn["syn_first_name"])
        .with_column("last_name", df_ln["syn_last_name"])
        .select(*cols)
    )

    return df
