PageRank
========
a class project exercising MapReduce programming



1. Describe your implementation of Project 2 and Project 3. (How & Why) 
A. Project 2
Mapper 에서 wiki의 xml chunk를 input 으로 받는데, <page> 단위로 한 덩어리씩 받는 다. 이것을 파싱해서 각 page의 title, id, 실제 text 내용 등을 받아낸다. 그 후 text에서 몇몇 가지 특수 문자를 골라낸 뒤 단어 별로 inverted index를 만들어서 reducer로 보 낸다.
Reducer 에서는 각각의 <단어, 타이틀> pair를 텍스트 출력으로 내보낸다. 

B. Project 3
  a. BuildOutlinks
Pagerank를 돌리기 위해 Preprocessing 하는 과정이다. Mapper 에서 wiki xml 청크 를 받아서 다른 페이지로 outgoing하는 링크를 찾아낸다. 그래서 <title, id, initial PageRank, number of outgoing links, outgoing links> 를 한 라인으로 텍스트로 출력 을 한다. Reducer는 identity reducer이다.
  b. PageRank
BuildOutlinks에서 출력한 파일을 한 라인씩 입력으로 받아 pagerank를 계산한다. Pagerank 계산은 PR(Pj) = (1-d) + d(sum of {PR(Pi) / C(Pi)}) 공식을 따르고 damping factor d = 0.85로 계산한다. Mapper에서는 두 가지 종류로 reducer에게 output을 보낸다. 첫번째는 내 page 안에 있는 outgoing link들의 페이지들에게 {PR(Pi) / C(Pi)} 를 계산해서 <title of Pj, {PR(Pi) / C(Pi)}> pair 들을 보낸다. 또한 자기 자신에게는 <title of Pi, (id, number of outgoing links, outgoing links)> 를 string으로 보낸다. 이렇게 함으로써 reducer에서는 d를 이용하여 pagerank 계산을 다 끝내고, mapper의 input으로 받은 그 형식대로 output을 내보낼 수 있게 된다. 이로써 PageRank를 여러 번 반복하며 iteration을 돌릴 수 있는 것이다.
실행이 너무 느려서 중간에 파싱 규칙을 조금 바꾸어서 pagerank 의 버전이 여러 개가 생겼다. buildoutlinks에서 받은 파일 포맷을 받을 수 있는 것은 PageRank, 이 것의 출력을 받는 것은 PageRankNext, 이 것의 출력을 받는 것은
PageRankNextAgain이다. PageRankNextAgain에서 나온 출력은 PageRankNextAgain에서 반복적으로 돌릴 수 있다.


2. Write how to execute your code.

Hadoop jar buildoutlinks.jar BuildOutlinks /corpus /user/st01/outlinks
Hadoop jar pagerank.jar PageRank /user/st01/outlinks /user/st01/pagerank_output
Hadoop jar pagerank2.jar PageRankNext /user/st01/pagerank_output /user/st01/pagerank_output2
Hadoop jar pagerank3.jar PageRankNextAgain /user/st01/pagerank_output2
이렇게 실행하면 됩니다. /user/st01/outlinks 에는 BuildOutlinks 에서 preprocessing 한 결 과 값들이 들어 있습니다. 중간 중간에 리팩토링 하면서 파일 입출력 포맷을 바꾸어서 PageRank의 버전이 여러 개가 생겨버렸습니다.


3. Give us your feedback on:

- What you’ve done & what you haven’t done.
과제에서 지시한대로 다 하긴 했는데 클러스터가 느리고 다운도 되고 해서 고생을 많이 했습니다. 그리고 문자열들을 그대로 다 내보내기 보다는 쓸데 없는 페이지들을 파싱해서 데이 터 용량을 좀 더 줄였으면 좋았겠다는 생각을 했습니다. 맵리듀스에서 new 와 arraylist를 쓰는 코스트가 얼마나 클지 예상을 못했던게 처음에 시간을 많이 먹은 이유 중 하나인 것 같습니다.
- How long you expected this project would take to finish and how much time you actually
spent on this project?
한 3~4일 걸릴 것이라고 예상했고 4일 이상 걸리고 있는 중입니다.
- Acknowledge any assistance you received from anyone and any online resource
그다지 도움될 만한 것들이 없었네요... hadoop이 ioexception 내고 뻗어 있는 동안 구글 링해서 왜 저러나 찾아보기는 했습니다. 주말 내내 클러스터가 계속 상태도 안좋고 뻗어 있어서
딜레이 하는데 한 몫 했습니다. ᅲᅲ
