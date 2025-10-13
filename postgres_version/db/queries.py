"""
데이터베이스 관련 SQL 쿼리들을 관리하는 모듈
"""
from psycopg2 import sql


class DatabaseQueries:
    """데이터베이스 쿼리들을 관리하는 클래스"""
    def create_database(database_name):
        """데이터베이스를 생성하는 쿼리"""
        return f"CREATE DATABASE IF NOT EXISTS {database_name};"
    
    @staticmethod
    def create_schema(schema_name):
        """스키마를 생성하는 쿼리"""
        return f"CREATE SCHEMA IF NOT EXISTS {schema_name};"
    
    @staticmethod
    def create_transit_table(schema_name, table_name):
        """서울 대중교통 패턴 테이블을 생성하는 쿼리"""
        return f"""
            CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
                id SERIAL PRIMARY KEY,
                create_date DATE NOT NULL,
                purpose_pattern TEXT,
                total_people INTEGER,
                kid_people INTEGER,
                youth_people INTEGER,
                elder_people INTEGER,
                disabled_people INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP
            );
        """
    
    @staticmethod
    def select_from_table(schema_name, table_name, limit=10):
        """테이블에서 데이터를 조회하는 쿼리"""
        return f"SELECT * FROM {schema_name}.{table_name} LIMIT {limit};"
    
    @staticmethod
    def insert_data_to_table(schema_name, table_name):
        """테이블에 데이터를 삽입하는 쿼리"""
        return f"""INSERT INTO {schema_name}.{table_name} (
                create_date,
                purpose_pattern,
                total_people,
                general_people,
                kid_people,
                youth_people,
                elder_people,
                disabled_people,
                created_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    
    @staticmethod
    def insert_batch_data_to_table(schema_name, table_name):
        """배치 데이터 삽입을 위한 쿼리 (execute_values 사용)"""
        return f"""INSERT INTO {schema_name}.{table_name} (
                create_date,
                purpose_pattern,
                total_people,
                general_people,
                kid_people,
                youth_people,
                elder_people,
                disabled_people
            )
            VALUES %s"""


class DatabaseCheckQueries:
    """데이터베이스 상태 확인을 위한 쿼리들"""
    @staticmethod
    def get_all_schemas():
        """모든 스키마 목록을 조회하는 쿼리"""
        return "SELECT schema_name FROM information_schema.schemata ORDER BY schema_name;"
    
    @staticmethod
    def check_schema_exists(schema_name):
        """특정 스키마가 존재하는지 확인하는 쿼리"""
        return "SELECT schema_name FROM information_schema.schemata WHERE schema_name = %s;", (schema_name,)
    
    @staticmethod
    def check_table_exists(schema_name, table_name):
        """특정 테이블이 존재하는지 확인하는 쿼리"""
        return """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = %s AND table_name = %s;
        """, (schema_name, table_name)
    
    @staticmethod
    def get_all_tables():
        """사용 가능한 모든 테이블 목록을 조회하는 쿼리"""
        return """
            SELECT table_schema, table_name 
            FROM information_schema.tables 
            WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
            ORDER BY table_schema, table_name;
        """
    
    @staticmethod
    def select_from_table_with_sql_identifier(schema_name, table_name):
        """psycopg2.sql.Identifier를 사용한 안전한 테이블 조회 쿼리"""
        return sql.SQL("SELECT * FROM {}.{}").format(
            sql.Identifier(schema_name),
            sql.Identifier(table_name)
        )


    @staticmethod
    def get_database_info():
        """데이터베이스 기본 정보를 조회하는 쿼리들"""
        return {
            'schemas': DatabaseCheckQueries.get_all_schemas(),
            'tables': DatabaseCheckQueries.get_all_tables()
        }
    
    @staticmethod
    def check_schema_and_table(schema_name, table_name):
        """스키마와 테이블 존재 여부를 확인하는 쿼리들"""
        return {
            'schema_check': DatabaseCheckQueries.check_schema_exists(schema_name),
            'table_check': DatabaseCheckQueries.check_table_exists(schema_name, table_name)
        }
