from typing import Set
from content.application.port.stopword_repository_port import StopwordRepositoryPort
from content.domain.text_preprocessor import TextPreprocessor
from content.application.usecase.text_preprocess_usecase import TextPreprocessUseCase
from config.database.session import SessionLocal

# 실제 DB Repository import (프로젝트 구조에 맞게 수정)
from content.infrastructure.repository.stopword_repository_impl import StopwordRepositoryImpl  # 실제 구현체 경로

# class InMemoryStopwordRepository(StopwordRepositoryPort):
#     def __init__(self, stopwords: Set[str]):
#         self._stopwords = stopwords

#     def get_stopwords(self, lang: str = "ko") -> Set[str]:
#         return self._stopwords


def test_text_preprocessor():

    #try:
        # 2. Repository 생성
        repo = StopwordRepositoryImpl()
        
        # 3. 전처리기 생성
        preprocessor = TextPreprocessor(stopword_repository=repo, lang="ko")
        
        # 4. UseCase 생성
        service = TextPreprocessUseCase(text_preprocessor=preprocessor)
        
        # 5. 테스트 입력
        raw = "정말 이 영상은 그냥 너무 좋다!!! 😊😊 그리고 내용도 최고임!!!"
        cleaned = service.clean_comment(raw)
        
        print("RAW:     ", raw)
        print("CLEANED: ", cleaned)
        print("Loaded stopwords count:", len(preprocessor.stopwords))
        
    #finally:
        #db.close()

    # 1. 인메모리 불용어 설정
    #repo = InMemoryStopwordRepository(stopwords={"그리고", "정말", "그냥"})

    # 1. 실제 DB Repository 인스턴스 생성 (DB 연결 필요)
    #repo = StopwordRepositoryImpl()

    # 2. 도메인 Preprocessor 생성
    #preprocessor = TextPreprocessor(stopword_repository=repo, lang="ko")

    # 3. 서비스 래퍼
    #service = TextPreprocessUseCase(text_preprocessor=preprocessor)

    # 4. 테스트 입력
    #raw = "정말 이 영상은 그냥 너무 좋다!!! 😊😊 그리고 내용도 최고임!!!"

    #cleaned = service.clean_comment(raw)

    #print("RAW:     ", raw)
    #print("CLEANED: ", cleaned)


if __name__ == "__main__":
    test_text_preprocessor()