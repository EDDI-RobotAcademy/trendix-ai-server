import re
from typing import Set
from content.infrastructure.repository.stopword_repository_impl import StopwordRepositoryImpl


class StopwordUseCase:
    """
    댓글/설명 텍스트에서
    - 특수문자 정리
    - 불용어 제거 (DB에서 로드)
    """

    def __init__(self, stopword_repository: StopwordRepositoryImpl, lang: str = "ko"):
        if stopword_repository is None:
            raise ValueError("stopword_repository must not be None")
        self.stopword_repository = stopword_repository
        self.lang = lang
        self.stopwords: Set[str] = set()
        self._load_stopwords()

        # 특수문자/이모지 등 제거용 정규식 (예시)
        # 한글/영문/숫자/공백만 남기고 나머지는 공백으로 치환
        self._allowed_pattern = re.compile(r"[^0-9a-zA-Z가-힣\s]+")

        # 다중 공백 제거 패턴
        self._multi_space_pattern = re.compile(r"\s+")

    def _load_stopwords(self):
        self.stopwords = self.stopword_repository.get_stopwords(self.lang)

    def reload_stopwords(self):
        """
        운영 중에 불용어가 변경되었을 때 재로드할 수 있는 메서드.
        """
        self._load_stopwords()
        
    def normalize_text(self, text: str) -> str:
        """
        특수문자 제거 + 공백 정리
        """
        if not text:
            return ""

        # 특수문자/이모지 제거
        text = self._allowed_pattern.sub(" ", text)

        # 대소문자 통일 (필요시)
        text = text.lower()

        # 다중 공백을 하나로
        text = self._multi_space_pattern.sub(" ", text).strip()

        return text

    def remove_stopwords_iterative(self, data):
        stack = [data]
        while stack:
            item = stack.pop()
            if isinstance(item, dict):
                for k, v in item.items():
                    if isinstance(v, (dict, list)):
                        stack.append(v)
                    elif isinstance(v, str):
                        item[k] = self.remove_stopwords(v)
            elif isinstance(item, list):
                for i, v in enumerate(item):
                    if isinstance(v, (dict, list)):
                        stack.append(v)
                    elif isinstance(v, str):
                        item[i] = self.remove_stopwords(v)
        return data        
        
    def remove_stopwords(self, text: str) -> str:
        """
        불용어 제거 (단어 단위)
        """
        if not text:
            return ""
        #print(f"self.stopwords={self.stopwords}")
        
         # 불용어 목록을 정규식 패턴으로 변환
        escaped_stopwords = [re.escape(word) for word in self.stopwords]
        pattern = '|'.join(escaped_stopwords)

        # 불용어를 빈 문자열로 치환
        result = re.sub(pattern, '', text)
        print(f"texts={text}, result={result}")
        return result

    def preprocess(self, text: str) -> str:
        """
        전체 전처리 pipeline:
        1) normalize_text
        2) remove_stopwords
        """
        normalized = self.normalize_text(text)
        return self.remove_stopwords(normalized)
