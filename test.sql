-- -*- sql-product: postgres; -*-

create extension if not exists "uuid-ossp";

create table if not exists person (
id uuid not null unique default uuid_generate_v4(),
last_name text,
first_name text,
birth_date date,
primary key (id));

create table if not exists address (
id serial primary key,
address text);

create table if not exists person_address (
id serial primary key,
person_id uuid not null references person (id),
address_id int not null references address (id));

select id from person;

insert into address (address) values ('703 15th Ave., SF, CA  94118');

insert into address (address) values ('235 Waller St., SF, CA');

insert into person_address (person_id, address_id) values ('62a3ad87-a5b7-456d-9256-8f6850715433', 1);

insert into person_address (person_id, address_id) values ('62a3ad87-a5b7-456d-9256-8f6850715433', 2);

delete from person_address where (id, person_id, address_id) in (
select id, person_id, address_id from person_address where person_id = '62a3ad87-a5b7-456d-9256-8f6850715433');


with t as (select address.id from address where address.id = 1) delete from address where address.id in (select * from t);

with t as (select person.id from person where true and person.id='62a3ad87-a5b7-456d-9256-8f6850715433' limit 10)
select * from t;

delete from person where (person.id) in (select * from t)






with t as (select person.id from person where true and person.id='62a3ad87-a5b7-456d-9256-8f6850715433' limit 10) update person set birth_date='2009-04-09', grade='4', last_name='Ventimiglia', id='62a3ad87-a5b7-456d-9256-8f6850715433', first_name='Olivia' where (person.id) in (select * from t)
