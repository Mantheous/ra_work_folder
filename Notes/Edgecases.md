# Edgecases
This is where I am trying to keep track of all of the weird behavior that I have seen so that I can make a more general system in the future.

## Cases
- Dordogne: Missing Cote, 1 page record that contained no genelogical information
    - Resolution: Discard records
- Records that can not be viewed
    - Resolution: Document somehow that the record was skipped.
- Missing results: Sometimes the website will just straight up not show a line of the table. It's just not there. Instead of there being 25 rows there are 24. Or 99 instead of 100. As far as I know there is no way to get these records. It is possible that they could be accessed via api. This would basically require figuring out their bug though. Kinda hard when you can't see the source code. Claude thought that the missing value was something, it gave me an api request and everything, but that record was already contained somewhere else in my scrape. So if that truely is the case then their website is just advertising that it has more records than it really has. I think I am just going to ignore them.