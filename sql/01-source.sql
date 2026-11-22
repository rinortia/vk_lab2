CREATE ROLE cdc WITH LOGIN REPLICATION PASSWORD 'cdc';
CREATE TABLE public.accounts (
    account_id BIGINT PRIMARY KEY,
    owner TEXT NOT NULL,
    city TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('active', 'blocked', 'closed'))
);
ALTER TABLE public.accounts REPLICA IDENTITY FULL;
INSERT INTO public.accounts VALUES
    (1001, 'Мария Соколова', 'Москва', 'active'),
    (1002, 'Илья Орлов', 'Казань', 'active');
GRANT CONNECT ON DATABASE bank TO cdc;
GRANT USAGE ON SCHEMA public TO cdc;
GRANT SELECT ON public.accounts TO cdc;
CREATE PUBLICATION bank_publication FOR TABLE public.accounts;
