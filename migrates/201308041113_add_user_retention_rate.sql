alter table fact_active_users add column one_days_retention_rate float default 0.0;
alter table fact_active_users add column three_days_retention_rate float default 0.0;
alter table fact_active_users add column seven_days_retention_rate float default 0.0;
alter table fact_active_users add column fourteen_days_retention_rate float default 0.0;
alter table fact_active_users add column thirty_days_retention_rate float default 0.0;