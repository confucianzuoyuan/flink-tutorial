TITLE="尚硅谷Flink教程"
SUBTITLE="Flink理论与项目实践"
AUTHOR='尚硅谷大数据教学组'
VERSION_DATE="v3.1.2, Copyright © November 13, 2019"

GUIDE_ID=flinktutorial

HTML=$(GUIDE_ID).html

PREPROC=../bin/preproc
PREPROC_MD=$(GUIDE_ID)_temp_preproc.md

COMMON_OPTS= \
	--variable title:$(TITLE) \
	--variable subtitle:$(SUBTITLE) \
	--variable author:$(AUTHOR) \
	--variable date:$(VERSION_DATE) \
	--toc

HTML_OPTS=$(COMMON_OPTS) \
	--metadata title:$(TITLE)

all: $(HTML)

$(GUIDE_ID).html: $(GUIDE_ID).md bg-css.html
	$(PREPROC) $< $(PREPROC_MD)
	pandoc $(HTML_OPTS) -s $(PREPROC_MD) -o $(GUIDE_ID).html -H bg-css.html
	sed 's/src="\(.*\)\.pdf"/src="\1.svg"/g' $(GUIDE_ID).html > $(GUIDE_ID)_temp.html # use svg images
	mv $(GUIDE_ID)_temp.html $(GUIDE_ID).html
	rm -f $(GUIDE_ID)_temp*

clean:
	rm -f $(GUIDE_ID)_temp*

pristine: clean
	rm -f $(HTML) $(BOOKS)

.PHONY: all, html, clean, pristine
