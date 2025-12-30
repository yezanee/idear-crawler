import logging
from typing import Dict, Optional
from datetime import date
import pymysql
from pymysql.cursors import DictCursor
from contextlib import contextmanager

logger = logging.getLogger(__name__)


class ContestRepository:
    # 공모전 데이터베이스 Repository
    
    def __init__(self, db_config: Dict):
        self.db_config = db_config
        self.connection = None
        self._connect()
    
    def _connect(self):
        # 데이터베이스 연결 생성
        try:
            self.connection = pymysql.connect(
                host=self.db_config['host'],
                port=self.db_config['port'],
                user=self.db_config['user'],
                password=self.db_config['password'],
                database=self.db_config['database'],
                charset='utf8mb4',
                cursorclass=DictCursor,
                autocommit=False,
                # Lambda 환경에 최적화된 타임아웃 설정
                connect_timeout=15,  # 초기 연결 타임아웃 증가 (VPC Cold Start 고려)
                read_timeout=60,     # 읽기 타임아웃 증가
                write_timeout=60     # 쓰기 타임아웃 증가
            )
            logger.info("Database connected successfully")
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise
    
    def _ensure_connection(self):
        # 연결 상태 확인 및 재연결
        try:
            if self.connection is None or not self.connection.open:
                logger.warning("Database connection lost, reconnecting...")
                self._connect()
            else:
                # 연결 테스트
                self.connection.ping(reconnect=True)
        except Exception as e:
            logger.error(f"Connection check failed: {e}")
            self._connect()
    
    @contextmanager
    def _get_cursor(self):
        # 커서 컨텍스트 매니저
        self._ensure_connection()
        cursor = self.connection.cursor()
        try:
            yield cursor
        finally:
            cursor.close()
    
    def exists_by_url(self, linkareer_url: str) -> bool:
        # URL로 공모전 존재 여부 확인
        try:
            with self._get_cursor() as cursor:
                sql = "SELECT COUNT(*) as cnt FROM contest WHERE linkareer_url = %s"
                cursor.execute(sql, (linkareer_url,))
                result = cursor.fetchone()
                return result['cnt'] > 0
        except Exception as e:
            logger.error(f"URL 존재 확인 실패: {e}")
            return False
    
    def exists_by_homepage_url(self, homepage_url: str) -> bool:
        # 홈페이지 URL로 공모전 존재 여부 확인

        if not homepage_url or not homepage_url.strip():
            return False
        
        try:
            with self._get_cursor() as cursor:
                sql = "SELECT COUNT(*) as cnt FROM contest WHERE homepage_url = %s"
                cursor.execute(sql, (homepage_url,))
                result = cursor.fetchone()
                return result['cnt'] > 0
        except Exception as e:
            logger.error(f"홈페이지 URL 존재 확인 실패: {e}")
            return False
    
    def save_if_not_duplicate(self, contest_data: Dict) -> bool:
        # 중복이 아닌 경우에만 공모전 저장
        try:
            # 링커리어 URL로 중복 체크 (필수)
            linkareer_url = contest_data.get('linkareer_url')
            if not linkareer_url:
                logger.warning("linkareer_url이 없습니다")
                return False
            
            if self.exists_by_url(linkareer_url):
                logger.debug(f"중복 (linkareer_url): {linkareer_url}")
                return False
            
            # 홈페이지 URL로 중복 체크 (선택)
            homepage_url = contest_data.get('homepage_url')
            if homepage_url and self.exists_by_homepage_url(homepage_url):
                logger.debug(f"중복 (homepage_url): {homepage_url}")
                return False
            
            # 저장
            with self._get_cursor() as cursor:
                sql = """
                    INSERT INTO contest 
                    (title, host, category, image_url, start_date, deadline, 
                     reward, description, linkareer_url, homepage_url, 
                     created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                """
                
                cursor.execute(sql, (
                    contest_data.get('title'),
                    contest_data.get('host'),
                    contest_data.get('category'),
                    contest_data.get('image_url'),
                    contest_data.get('start_date'),
                    contest_data.get('deadline'),
                    contest_data.get('reward'),
                    contest_data.get('description'),
                    linkareer_url,
                    homepage_url
                ))
                
                self.connection.commit()
                logger.debug(f"공모전 저장 성공: {contest_data.get('title')}")
                return True
                
        except Exception as e:
            logger.error(f"공모전 저장 실패: {e}", exc_info=True)
            try:
                self.connection.rollback()
            except:
                pass
            return False
    
    def save_batch(self, contests: List[Dict]) -> tuple:
        # 배치로 공모전 저장 (성능 최적화)
        if not contests:
            return 0, 0
        
        try:
            # 1. 중복 체크 (기존 URL 조회)
            linkareer_urls = [c.get('linkareer_url') for c in contests if c.get('linkareer_url')]
            
            if not linkareer_urls:
                logger.warning("유효한 linkareer_url이 없습니다")
                return 0, len(contests)
            
            # 기존 URL 조회
            with self._get_cursor() as cursor:
                placeholders = ','.join(['%s'] * len(linkareer_urls))
                sql = f"SELECT linkareer_url FROM contest WHERE linkareer_url IN ({placeholders})"
                cursor.execute(sql, linkareer_urls)
                existing_urls = {row['linkareer_url'] for row in cursor.fetchall()}
            
            # 2. 새로운 공모전만 필터링
            new_contests = [
                c for c in contests 
                if c.get('linkareer_url') and c.get('linkareer_url') not in existing_urls
            ]
            
            if not new_contests:
                logger.info(f"모두 중복: {len(contests)}개")
                return 0, len(contests)
            
            # 3. 배치 INSERT
            with self._get_cursor() as cursor:
                sql = """
                    INSERT INTO contest 
                    (title, host, category, image_url, start_date, deadline, 
                     reward, description, linkareer_url, homepage_url, 
                     created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                """
                
                values = [
                    (
                        c.get('title'),
                        c.get('host'),
                        c.get('category'),
                        c.get('image_url'),
                        c.get('start_date'),
                        c.get('deadline'),
                        c.get('reward'),
                        c.get('description'),
                        c.get('linkareer_url'),
                        c.get('homepage_url')
                    )
                    for c in new_contests
                ]
                
                cursor.executemany(sql, values)
                self.connection.commit()
            
            saved_count = len(new_contests)
            duplicate_count = len(contests) - saved_count
            
            logger.info(f"배치 저장 완료: 저장={saved_count}, 중복={duplicate_count}")
            return saved_count, duplicate_count
            
        except Exception as e:
            logger.error(f"배치 저장 실패: {e}", exc_info=True)
            try:
                self.connection.rollback()
            except:
                pass
            return 0, len(contests)
    
    def delete_closed_contests(self, today: date) -> int:
        # 마감된 공모전 삭제

        try:
            with self._get_cursor() as cursor:
                sql = "DELETE FROM contest WHERE deadline < %s"
                affected = cursor.execute(sql, (today,))
                self.connection.commit()
                logger.info(f"마감된 공모전 {affected}개 삭제")
                return affected
        except Exception as e:
            logger.error(f"마감 공모전 삭제 실패: {e}")
            try:
                self.connection.rollback()
            except:
                pass
            return 0
    
    def count(self) -> int:
        # 전체 공모전 개수 조회
        try:
            with self._get_cursor() as cursor:
                sql = "SELECT COUNT(*) as cnt FROM contest"
                cursor.execute(sql)
                result = cursor.fetchone()
                return result['cnt']
        except Exception as e:
            logger.error(f"공모전 개수 조회 실패: {e}")
            return 0
    
    def close(self):
        # 데이터베이스 연결 종료
        if self.connection:
            try:
                self.connection.close()
                logger.info("Database connection closed")
            except Exception as e:
                logger.error(f"Database close error: {e}")
