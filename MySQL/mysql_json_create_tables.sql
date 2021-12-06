drop database if exists movie_json_test;
create database movie_json_test;

create user 'movie_json_user'@'localhost' identified by 'Ch@nge_me_1st';
GRANT create,alter,drop,select,insert,update,delete,index on  movie_json_test.* to 'movie_json_user'@'localhost';  
use movie_json_test;

drop table if exists movies_json;

drop table if exists movies_json_hybrid;
drop table if exists movies_json_generated;;


drop table if exists movies_normalized_meta;
drop table if exists movies_normalized_actors;
drop table if exists movies_normalized_cast;


create table movies_json (
	ai_myid int AUTO_INCREMENT primary key, 
	imdb_id varchar(255),
	json_column json
) engine = innodb;

create unique index imdb_idx on movies_json(imdb_id);

create table movies_json_generated (
	ai_myid int AUTO_INCREMENT primary key, 
	imdb_id varchar(255) generated always as (`json_column` ->> '$.imdb_id'),
	title varchar(255) generated always as (`json_column` ->> '$.title'),
    imdb_rating decimal(5,2) generated always as (json_value(json_column, '$.imdb_rating')) ,
	overview text generated always as (`json_column` ->> '$.overview'),
	director json generated always as (`json_column` ->> '$.director'),
	country varchar(100) generated always as (`json_column` ->> '$.country'),
	json_column json
) engine = innodb;

create unique index imdb_idx on movies_json_generated(imdb_id);
create index title_idx on movies_json_generated(title);
	

create table movies_normalized_meta (
	ai_myid int AUTO_INCREMENT primary key, 
	imdb_id varchar(32), 
	title varchar(255), 
	imdb_rating decimal(5,2),
	year int,
	country varchar(100),
	overview text,
	json_column json,
	upvotes int default 0,
	downvotes int default 0
) engine = innodb;

create unique index imdb_id_idx  on movies_normalized_meta (imdb_id);
create index rating_idx  on movies_normalized_meta (imdb_rating);

create table movies_normalized_actors (
	ai_actor_id int auto_increment primary key, 
	actor_id varchar(50), 
	actor_name varchar(500)
	) engine = innodb;
create unique index actor_id_idx  on movies_normalized_actors (actor_id);
create index actor_name_idx  on movies_normalized_actors (actor_name);

create table movies_normalized_cast (
	inc_id int auto_increment primary key, 
	ai_actor_id int, 
	ai_myid int,
        actor_character varchar(500)	
	) engine = innodb;

create index cast_id_idx  on movies_normalized_cast (ai_actor_id,ai_myid);
create index cast_id2_idx  on movies_normalized_cast (ai_myid);
create index cast_character_idx  on movies_normalized_cast (actor_character);
create unique index u_cast_idx  on movies_normalized_cast (ai_myid,ai_actor_id,actor_character);
	
create table movies_normalized_director (
  director_id int auto_increment primary key, 
  ai_myid int,
  director varchar(500)	
) engine = innodb;

create index director_mv_idx  on movies_normalized_director (ai_myid);

create table movies_normalized_genres_tags (
	  genre_id int auto_increment primary key, 
	  genre varchar(500)	
	) engine = innodb;
create unique index genre_idx  on movies_normalized_genres_tags(genre);

	
create table movies_normalized_genres (
	  genre_id int, 
	  ai_myid int
) engine = innodb;	

alter table movies_normalized_genres add primary key (genre_id,ai_myid);

	
create table movies_normalized_user_comments(
  comment_id int auto_increment primary key, 
  ai_myid int,
  rating int,
  comment text,
  imdb_id varchar(20),
  comment_add_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  comment_update_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP
) engine = innodb;	


create index comment_movie_id_idx  on movies_normalized_user_comments (ai_myid);


create table movies_normalized_aggregate_ratings(
	  ai_myid int primary key,
	  user_rating int,
	  up_votes int,
	  down_votes int,
	  imdb_rating int
) engine = innodb;	

CREATE TABLE `voting_count_history` (
  `ai_myid` int NOT NULL,
  `store_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `title` varchar(255) NOT NULL,
  `imdb_id` varchar(20) DEFAULT NULL,
  `comment_count` bigint NOT NULL DEFAULT '0',
  `max_rate` int DEFAULT NULL,
  `avg_rate` decimal(14,4) DEFAULT NULL,
  `upvotes` decimal(32,0) DEFAULT NULL,
  `downvotes` decimal(32,0) DEFAULT NULL,
  PRIMARY KEY (`title`,`ai_myid`,`store_time`),
  KEY `ai_myid` (`ai_myid`)
) ENGINE=InnoDB;
