import logging
import time
from typing import List
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import json

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

        try:
            logger.info(f"{page}페이지 API 호출 시작")
            
            # 1페이지는 28개, 2페이지부터는 20개로 고정
            page_size = 28 if page == 1 else 20
            
            # GraphQL 쿼리 파라미터
            params = {
                'operationName': 'ActivityList_Activities',
                'variables': json.dumps({
                    'filterBy': {
                        'status': 'OPEN',
                        'activityTypeID': '3'  # 3 = 공모전
                    },
                    'pageSize': page_size,
                    'page': page,
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
            
            # URL 목록 생성
            urls = []
            for activity in activities:
                activity_id = activity.get('id')
                if activity_id:
                    url = f"{self.BASE_URL}/activity/{activity_id}"
                    urls.append(url)
            
            logger.info(f"{page}페이지에서 {len(urls)}개의 URL 발견")
            return urls
            
        except Exception as e:
            logger.error(f"{page}페이지 크롤링 실패: {str(e)}", exc_info=True)
            return []
    
    def cleanup(self):
        # 리소스 정리
        if hasattr(self, 'session'):
            try:
                self.session.close()
            except:
                pass
