"""
Pagination utilities for Linkareer crawler.
"""

import logging
from typing import Dict, List, Optional, Any

logger = logging.getLogger(__name__)


class PaginationConfig:
    """페이지네이션 설정 상수"""
    
    FIRST_PAGE_SIZE = 28
    DEFAULT_PAGE_SIZE = 20
    MAX_PAGE_LIMIT = 100
    DEFAULT_MAX_PAGES = 50
    EMPTY_PAGE_THRESHOLD = 3


class PaginationValidator:
    """페이지네이션 유효성 검사기"""
    
    @staticmethod
    def validate_page_number(page: int, max_limit: int = PaginationConfig.MAX_PAGE_LIMIT) -> bool:
        """페이지 번호 유효성 검사"""
        if page < 1:
            logger.error(f"잘못된 페이지 번호: {page} (1부터 시작해야 함)")
            return False
        
        if page > max_limit:
            logger.warning(f"페이지 번호가 상한 초과: {page} > {max_limit}")
            return False
        
        return True
    
    @staticmethod
    def validate_pagination_range(current_page: int, actual_items_count: int, expected_page_size: int) -> Dict[str, Any]:
        """페이지네이션 범위 유효성 검사 및 정보 반환"""
        validation_result = {
            'is_valid': True,
            'warnings': [],
            'page_info': {
                'current_page': current_page,
                'actual_items_count': actual_items_count,
                'expected_size': expected_page_size,
                'size_mismatch': False
            }
        }
        
        actual_size = validation_result['page_info']['actual_items_count']
        
        if actual_size != expected_page_size:
            validation_result['page_info']['size_mismatch'] = True
            validation_result['warnings'].append(f"페이지 크기 불일치: 예상 {expected_page_size}, 실제 {actual_size}")
            
            if actual_size == 0:
                validation_result['warnings'].append(f"페이지 {current_page}가 비어있음 - 다음 페이지 확인 필요")
            elif actual_size < expected_page_size:
                validation_result['warnings'].append(f"페이지 {current_page}가 부분적으로 채워짐 - 마지막 페이지일 수 있음")
        
        return validation_result
    
    @staticmethod
    def get_page_size(page: int) -> int:
        """페이지 크기 계산 (API 제약사항)"""
        if page < 1:
            raise ValueError(f"페이지 번호는 1 이상이어야 함: {page}")
        
        # 1페이지: 28개, 2페이지 이상: 20개
        page_size = PaginationConfig.FIRST_PAGE_SIZE if page == 1 else PaginationConfig.DEFAULT_PAGE_SIZE
        logger.debug(f"페이지 {page}: 크기 {page_size}")
        return page_size
    
    @staticmethod
    def get_expected_items_for_pages(total_pages: int) -> int:
        """지정된 페이지 수에 대한 예상 아이템 수 계산"""
        if total_pages <= 0:
            return 0
        
        first_page_items = PaginationConfig.FIRST_PAGE_SIZE
        remaining_pages = max(0, total_pages - 1)
        remaining_items = remaining_pages * PaginationConfig.DEFAULT_PAGE_SIZE
        
        return first_page_items + remaining_items
    
    @staticmethod
    def get_api_page_number(user_page: int) -> int:
        """사용자 페이지 번호를 API 페이지 번호로 변환 (1-based -> 0-based)"""
        if user_page < 1:
            raise ValueError(f"잘못된 페이지 번호: {user_page}")
        return user_page - 1
    
    @staticmethod
    def calculate_max_pages(user_max_pages: Optional[int]) -> int:
        """최대 페이지 수 계산"""
        default_limit = PaginationConfig.DEFAULT_MAX_PAGES
        absolute_limit = PaginationConfig.MAX_PAGE_LIMIT
        
        if user_max_pages is None:
            return min(default_limit, absolute_limit)
        
        return min(user_max_pages, absolute_limit)


class PaginationState:
    """페이지네이션 상태 관리"""
    
    def __init__(self):
        self.empty_page_count = 0
        self.consecutive_duplicates = 0
        self.processed_urls = set()
    
    def should_stop_on_empty_pages(self) -> bool:
        """연속 빈 페이지로 중지 여부"""
        return self.empty_page_count >= PaginationConfig.EMPTY_PAGE_THRESHOLD
    
    def should_stop_on_duplicates(self, threshold: int) -> bool:
        """연속 중복으로 중지 여부"""
        return self.consecutive_duplicates >= threshold
    
    def reset_empty_counter(self):
        """빈 페이지 카운터 리셋"""
        self.empty_page_count = 0
    
    def reset_duplicate_counter(self):
        """중복 카운터 리셋"""
        self.consecutive_duplicates = 0
    
    def increment_empty_counter(self):
        """빈 페이지 카운터 증가"""
        self.empty_page_count += 1
    
    def increment_duplicate_counter(self):
        """중복 카운터 증가"""
        self.consecutive_duplicates += 1
    
    def is_url_processed(self, url: str) -> bool:
        """URL 처리 여부 확인"""
        return url in self.processed_urls
    
    def mark_url_processed(self, url: str):
        """URL을 처리됨으로 표시"""
        self.processed_urls.add(url)


def create_pagination_summary(total_pages: int, total_items: int, execution_time: float) -> Dict:
    """페이지네이션 요약 정보 생성"""
    return {
        'pages_processed': total_pages,
        'total_collected': total_items,
        'execution_time': round(execution_time, 2),
        'avg_items_per_page': round(total_items / total_pages, 2) if total_pages > 0 else 0
    }