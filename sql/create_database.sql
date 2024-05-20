--setup database
drop DATABASE IF EXISTS demo;
create DATABASE demo;
\c demo;

drop table if exists public.result;

create table if not exists public.result (
id BIGINT NOT NULL,
calculated_value DOUBLE PRECISION NOT NULL,
write_side_offset BIGINT NOT NULL,
PRIMARY KEY(id)
);

INSERT INTO public.result VALUES (1,0,1);


