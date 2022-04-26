-- 시퀀스 생성
CREATE SEQUENCE seq_data;

-- 테이블 생성
CREATE TABLE public.data
(
    mbr_no bigint not null default nextval('seq_data') ,
    value text COLLATE pg_catalog."default",
    receivedtime text COLLATE pg_catalog."default"
)

-- 테이블 삭제시 시퀀스도 같이 삭제 처리
ALTER SEQUENCE seq_data OWNED BY public.data.mbr_no;