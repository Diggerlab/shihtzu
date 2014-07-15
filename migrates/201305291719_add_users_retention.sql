alter table fact_active_users add column one_days_retention integer default 0;
alter table fact_active_users add column three_days_retention integer default 0;
alter table fact_active_users add column seven_days_retention integer default 0;