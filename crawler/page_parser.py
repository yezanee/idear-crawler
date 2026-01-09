import logging
import time
from typing import List
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import json
from .pagination import PaginationValidator, PaginationConfig
from . import filter_config

logger = logging.getLogger(__name__)


class LinkareerPageParser:
    # 링커리어 목록 페이지 파서 (GraphQL API 사용)
    
    BASE_URL = "https://linkareer.com"
    API_URL = "https://api.linkareer.com/graphql"
    
    def __init__(self):
        self.session = requests.Session()

        # Retry 정책 적용
        retry_strategy = Retry(
            total=3,
            backoff_factor=0.3,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount('https://', adapter)
        self.session.mount('http://', adapter)

        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': '*/*',
            'Origin': 'https://linkareer.com',
            'Referer': 'https://linkareer.com/list/contest'
        })
    
    def parse_list_page(self, page: int) -> List[str]:
        # 목록 페이지에서 공모전 URL 추출 (GraphQL API 사용)
        
        # 페이지 번호 유효성 검사
        if not PaginationValidator.validate_page_number(page):
            return []

        try:
            logger.info(f"{page}페이지 API 호출 시작")
            
            # 페이지 크기 결정 (API 제약사항)
            page_size = PaginationValidator.get_page_size(page)
            logger.debug(f"페이지 {page}: 크기 {page_size}")
            
            # API 페이지 파라미터 변환
            api_page = PaginationValidator.get_api_page_number(page)
            
            # GraphQL 쿼리 파라미터
            params = {
                'operationName': 'ActivityList_Activities',
                'variables': json.dumps({
                    'filterBy': {
                        'status': 'OPEN',
                        'activityTypeID': '3'  # 3 = 공모전
                    },
                    'pageSize': page_size,
                    'page': api_page,  # 0-based 인덱스로 변환
                    'activityOrder': {
                        'field': 'CREATED_AT',
                        'direction': 'DESC'
                    }
                }, separators=(',', ':')),
                'extensions': json.dumps({
                    'persistedQuery': {
                        'version': 1,
                        'sha256Hash': 'f59df641666ef9f55c69ed6a14866bfd2f87fb32c89a80038a466b201ee11422'
                    }
                }, separators=(',', ':'))
            }
            
            logger.debug(f"API 호출: page={api_page} (사용자 페이지={page}), pageSize={page_size}")
            
            # API 호출
            response = self.session.get(
                self.API_URL,
                params=params,
                timeout=10
            )
            
            if response.status_code != 200:
                logger.error(f"API 호출 실패: HTTP {response.status_code}")
                return []
            
            data = response.json()
            
            # 데이터 추출
            activities = data.get('data', {}).get('activities', {}).get('nodes', [])
            
            if not activities:
                logger.info(f"{page}페이지에 데이터 없음")
                return []
            
            # URL 목록 생성 (Stage 1 필터링 적용)
            urls = []
            stage1_filtered = 0
            
            for activity in activities:
                activity_id = activity.get('id')
                if not activity_id:
                    continue
                
                # Stage 1: 목록 페이지에서 1차 필터링
                # API 응답에 title이 있다면 사용, 없으면 필터링 스킵
                if self._should_skip_activity(activity):
                    stage1_filtered += 1
                    title = activity.get('title', activity_id)
                    logger.debug(f"1차 필터링: {title} (ID: {activity_id})")
                    continue
                
                url = f"{self.BASE_URL}/activity/{activity_id}"
                urls.append(url)
            
            if stage1_filtered > 0:
                logger.info(f"{page}페이지: {len(activities)}개 중 {stage1_filtered}개 1차 필터링, {len(urls)}개 상세 크롤링 예정")
            else:
                logger.info(f"{page}페이지에서 {len(urls)}개의 URL 발견")
            
            return urls
            
        except Exception as e:
            logger.error(f"{page}페이지 크롤링 실패: {str(e)}", exc_info=True)
            return []
    
    def _should_skip_activity(self, activity: dict) -> bool:
        """
        Stage 1: 목록 페이지에서 1차 필터링
        
        명확한 키워드만 사용하여 보수적으로 필터링 (오탐 방지)
        - 테스트 데이터
        - 명확한 비공모전 (마라톤, 채용 등)
        
        Args:
            activity: API 응답의 activity 객체
        
        Returns:
            bool: 필터링해야 하면 True, 통과시키면 False
        """
        # API 응답에서 title 추출 (없으면 필터링 스킵)
        title = activity.get('title', '')
        if not title or not isinstance(title, str):
            return False  # title이 없으면 필터링하지 않음 (보수적 접근)
        
        title_lower = title.lower()
        
        # 1. 명확한 테스트 데이터 필터링
        for keyword in filter_config.STAGE1_TEST_KEYWORDS:
            if keyword in title_lower:
                logger.debug(f"Stage 1 필터링 (테스트): '{title}' (키워드: {keyword})")
                return True
        
        # 2. 명확한 비공모전 필터링
        for keyword in filter_config.STAGE1_NON_IDEA_KEYWORDS:
            if keyword in title_lower:
                logger.debug(f"Stage 1 필터링 (비공모전): '{title}' (키워드: {keyword})")
                return True
        
        # 모든 검사 통과 - 상세 크롤링 진행
        return False
    
    def cleanup(self):
        # 리소스 정리
        if hasattr(self, 'session'):
            try:
                self.session.close()
            except:
                pass
