options mprint symbolgen;
%macro import(subr);
	x "rm /path/to/save/reddit/csv/&subr..csv || True";
	x "cd /path/to/reddit_extract; /usr/bin/python3.6 /path/to/reddit_extract/extract.py &subr. /path/to/save/reddit/csv/";

	data work.&subr.;
    infile "/path/to/save/reddit/csv/&subr..csv"
    delimiter = ","
    missover dsd
    firstobs=2
    lrecl=32767;

    length title $255.
    	   score 8
    	   id $10.
    	   url $255.
    	   comments 8
    	   created 8
    	   body $255.
    	   ts_str $20.;

    input title $
    	  score
    	  id $
    	  url $
    	  comments
    	  created
    	  body $
    	  ts_str $;

    format title $255.
    	   score 15.
    	   id $10.
    	   url $255.
    	   comments 10.
    	   created 15.
    	   body $255.
    	   ts_str $25.;

    created=input(ts_str,anydtdtm.);
	format created datetime18.;

	drop ts_str;

	format subreddit $25.;
	subreddit = "&subr.";
	run;

	proc append base=reddit data=&subr.; run;
%mend import;

%import(wallstreetbets);
%import(SecurityAnalysis);
%import(StockMarket);
%import(investing);
%import(algotrading);
%import(options);

proc sort data=reddit nodupkey;
	by id;
run;

proc sort data=reddit;
	by descending created;
run;

proc freq data=reddit;
	table subreddit;
run;

%post_process;
