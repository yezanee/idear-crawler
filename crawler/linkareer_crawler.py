import time
import logging
from typing import Dict, Set, Optional, List
from datetime import date

from .page_parser import LinkareerPageParser
from .detail_parser import ContestDetailParser
from .pagination import PaginationValidator, PaginationState, PaginationConfig, create_pagination_summary

logger = logging.getLogger(__name__)


class LinkareerCrawler:
    # 링커리어 공모전 크롤러 (S3 저장용)
    
    def __init__(self):
        self.page_parser = None
        self.detail_parser = ContestDetailParser()
        # self.pagination_state = PaginationState()  # __init__에서 한 번만 초기화
    
    def initial_backfill(self, max_pages: Optional[int] = None) -> Dict:
        # 초기 백필 - 모든 공모전 데이터 수집
        start_time = time.time()
        logger.info("=== 초기 백필 시작 ===")

        all_contests = []
        
        # PaginationState 상태 초기화 (중복 방지)
        self.pagination_state.reset_empty_counter()
        self.pagination_state.reset_duplicate_counter()
        # processed_urls는 그대로 유지 (새로 초기화하지 않음)
        # self.pagination_state = PaginationState()  # __init__에서 한 번만 초기화

        try:
            self.page_parser = LinkareerPageParser()

            page = 1
            max_pages_limit = PaginationValidator.calculate_max_pages(max_pages)
            
            logger.info(f"최대 페이지 수: {max_pages_limit}")

            while page <= max_pages_limit:
                # 페이지 크롤링 시작
                expected_items = PaginationValidator.get_page_size(page)
                logger.debug(f"페이지 {page} 크롤링 시작 (예상 {expected_items}개)")

                urls = self.page_parser.parse_list_page(page)

                if not urls:
                    self.pagination_state.increment_empty_counter()
                    logger.warning(f"{page}페이지에 데이터 없음 (연속 빈 페이지: {self.pagination_state.empty_page_count})")
                    
                    if self.pagination_state.should_stop_on_empty_pages():
                        logger.warning("연속 3페이지가 비어있어 백필 조기 종료")
                        break
                    
                    page += 1
                    continue
                else:
                    self.pagination_state.reset_empty_counter()
                    actual_items = len(urls)
                    
                    # 페이지네이션 유효성 검사
                    validation = PaginationValidator.validate_pagination_range(page, actual_items, expected_items)
                    
                    if validation['warnings']:
                        for warning in validation['warnings']:
                            logger.warning(f"페이지 {page}: {warning}")
                    
                    logger.info(f"페이지 {page} 완료: {actual_items}/{expected_items}개 수집")

                # 배치 크롤링
                page_contests = self._crawl_batch(urls)
                all_contests.extend(page_contests)
                pages_processed += 1

                logger.info(f"페이지 {page} 완료: {len(page_contests)}개 수집")

                page += 1
                time.sleep(0.5)

            if page > max_pages_limit:
                logger.warning(f"초기 백필이 상한 {max_pages_limit}페이지에 도달했습니다.")

            execution_time = time.time() - start_time
            logger.info(f"=== 초기 백필 완료 (총 {len(all_contests)}개 수집, {execution_time:.2f}초) ===")

            return {
                'contests': all_contests,
                'total_collected': len(all_contests),
                'pages_processed': pages_processed,
                'execution_time': round(execution_time, 2)
            }

        except Exception as e:
            logger.error(f"초기 백필 실패: {str(e)}", exc_info=True)
            raise
    
    def daily_update(self) -> Dict:
        # 일일 업데이트 - 최신 공모전만 수집
        start_time = time.time()
        logger.info("=== 일일 업데이트 시작 ===")
        
        try:
            # 최신 공모전 크롤링 (첫 몇 페이지만)
            self.page_parser = LinkareerPageParser()
            contests = self._crawl_new_contests()
            
            execution_time = time.time() - start_time
            logger.info(f"=== 일일 업데이트 완료 (총 {len(contests)}개 수집, {execution_time:.2f}초) ===")
            
            return {
                'contests': contests,
                'total_collected': len(contests),
                'execution_time': round(execution_time, 2)
            }
            
        except Exception as e:
            logger.error(f"일일 업데이트 실패: {str(e)}", exc_info=True)
            raise
    
    # 헬퍼 메서드들

    def _crawl_new_contests(self, max_pages: int = 5, stop_on_duplicates: int = 3) -> List[Dict]:
        # 새 공모전 크롤링 (일일 업데이트용)
        logger.info(f"=== 새 공모전 크롤링 시작 (최대 {max_pages}페이지, 연속 중복 {stop_on_duplicates}개 시 종료) ===")
        
        all_contests = []
        page = 1
        
        # PaginationState 카운터 초기화 (기존 URL은 유지)
        self.pagination_state.reset_empty_counter()
        self.pagination_state.reset_duplicate_counter()

        # 최대 페이지 수 제한 (안전장치)
        max_pages_limit = PaginationValidator.calculate_max_pages(max_pages)  # LinkareerPageParser.MAX_PAGE_LIMIT와 동일

        while page <= max_pages_limit:
            # 페이지 크롤링 시작
            
            if not self.page_parser:
                logger.error("Page parser가 초기화되지 않음")
                break
                
            urls = self.page_parser.parse_list_page(page)
            
            if not urls:
                empty_page_count += 1
                logger.info(f"{page}페이지에 데이터 없음 (연속 빈 페이지: {empty_page_count})")
                
                if empty_page_count >= 3:
                    logger.warning("연속 3페이지가 비어있어 크롤링 조기 종료")
                    break
                    
                page += 1
                continue
            else:
                empty_page_count = 0
            
            # 배치 크롤링 (중복 체크 포함)
            page_contests, duplicates_found = self._crawl_batch_with_duplicate_check(urls)
            all_contests.extend(page_contests)
            
            logger.info(f"페이지 {page} 완료: {len(page_contests)}개 수집, {duplicates_found}개 중복")
            
            # 중복 체크: 페이지 전체가 중복이면 연속 중복 카운트 증가
            if duplicates_found > 0 and len(page_contests) == 0:
                self.pagination_state.increment_duplicate_counter()
                logger.info(f"연속 중복 페이지: {consecutive_duplicates}/{stop_on_duplicates}")
                
                if self.pagination_state.should_stop_on_duplicates(stop_on_duplicates):
                    logger.info(f"연속 {consecutive_duplicates}개 페이지가 모두 중복 → 크롤링 조기 종료")
                    break
                else:
                    # 새 공모전이 있으면 카운터 리셋
                    self.pagination_state.reset_duplicate_counter()                # 새 공모전이 있으면 카운터 리셋
                # 기존 consecutive_duplicates는 PaginationState에서 관리
            
            page += 1
            time.sleep(0.5)

        logger.info(f"=== 새 공모전 크롤링 완료 (총 {len(all_contests)}개 수집) ===")
        return all_contests
    
    def _crawl_batch(self, urls: List[str]) -> List[Dict]:
        # URL 목록 목록을 배치로 크롤링
        contests = []
        success_count = 0
        fail_count = 0
        
        for url in urls:
            contest_data = self._crawl_single(url)
            if contest_data:
                contests.append(contest_data)
                success_count += 1
            else:
                fail_count += 1
            
            # 요청 간격
            time.sleep(0.3)
        
        # 배치 요약 로그
        logger.info(f"배치 크롤링 완료: 성공={success_count}, 실패={fail_count}, 총={len(urls)}개")
        
        return contests
    
    def _crawl_batch_with_duplicate_check(self, urls: List[str]) -> tuple:
        # URL 목록을 배치로 크롤링 (중복 체크 포함)
        contests = []
        success_count = 0
        duplicate_count = 0
        fail_count = 0
        
        for url in urls:
            # 중복 체크 (메모리)
            if self.pagination_state.is_url_processed(url):
                duplicate_count += 1
                logger.debug(f"중복 URL 건너뜀: {url}")
                continue
            
            contest_data = self._crawl_single(url)
            if contest_data:
                contests.append(contest_data)
                success_count += 1
            else:
                fail_count += 1
            
            # 요청 간격
            time.sleep(0.3)
        
        # 배치 요약 로그
        logger.info(f"배치 크롤링 완료: 성공={success_count}, 중복={duplicate_count}, 실패={fail_count}, 총={len(urls)}개")
        
        return contests, duplicate_count
    
    def _crawl_single(self, url: str, max_retries: int = 3) -> Optional[Dict]:
        # 단일 URL 크롤링 (재시도 로직 포함)
        # 메모리 중복 체크
        if self.pagination_state.is_url_processed(url):
            logger.debug(f"이미 처리한 URL: {url}")
            return None
        
        # 재시도 로직
        for attempt in range(max_retries):
            try:
                # 상세 정보 크롤링
                contest_data = self.detail_parser.parse_detail_page(url)
                
                if contest_data:
                    logger.info(f"공모전 수집 성공: {contest_data.get('title', 'Unknown')}")
                    self.pagination_state.mark_url_processed(url)
                    return contest_data
                else:
                    logger.debug(f"데이터 없음: {url}")
                    self.pagination_state.mark_url_processed(url)
                    return None
                    
            except Exception as e:
                if attempt == max_retries - 1:
                    # 최종 실패
                    logger.error(f"크롤링 최종 실패 ({max_retries}회 재시도): {url} - {str(e)}")
                    self.pagination_state.mark_url_processed(url)
                    return None
                else:
                    # 재시도
                    logger.warning(f"크롤링 실패 (재시도 {attempt + 1}/{max_retries}): {url} - {str(e)}")
                    time.sleep(1)  # 재시도 전 대기
        
        return None
    
    def cleanup(self):
        # 리소스 정리
        logger.info("리소스 정리 중...")
        
        if self.page_parser:
            try:
                self.page_parser.cleanup()
                logger.info("Page parser 정리 완료")
            except Exception as e:
                logger.error(f"Page parser 정리 오류: {e}")
        
        if self.detail_parser:
            try:
                self.detail_parser.cleanup()
                logger.info("Detail parser 정리 완료")
            except Exception as e:
                logger.error(f"Detail parser 정리 오류: {e}")
