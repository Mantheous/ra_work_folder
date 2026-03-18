# TODO
- [ ] Follow up on Aube1
- [ ] Find new approach to  scraping 20000+ record sites
    - [x] One option is to do it by commune and then just dedupe. This make validation very difficult though.
- [x] Create an async scraper
- [ ] Reimplement other scrapers
    - [x] Cher
    - [x] Creuse
        - Definently split on marriages. That way I can just run an almost unmodified scrapper that just has two starting urls. It's much easier to validate because it only has two with marraiges, and without.
    - [ ] Dordogne
        - Waiting on response from them. Mostly trying to see if maybe I can just not scrape it.
    - [ ] Eure-et-Loir
    - [ ] Indre
    - [ ] Landes
    - [ ] Loiret
    - [ ] Saint Etienne
- [ ] Develop a process for handling the edge cases on validating the data
- [ ] review the async scraper
- [ ] Remove the log on download. That is slowing down the scraper by 200%

## CCC Records
- [x] Get API key
- [x] Find method to get extracted text
- [x] Find method to split cases
- [x] Plan structure for search data
- [x] Prove the silent failure on network load.
- [ ] Get a sample of a case
- [ ] Do a basic NLP extraction on the sample case
- [ ] Figure out how I want to structure the personal data

- [ ] Small scope Question: Names of men in company 4479.
    - [ ] extracted text query
    - [ ] split pages by person
    - [ ] parse response
    - [ ] NLP to extract names
