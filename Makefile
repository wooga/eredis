APP=eredis

.PHONY: all compile clean Emakefile

all: compile

compile: ebin/$(APP).app Emakefile
	erl -noinput -eval 'up_to_date = make:all()' -s erlang halt

clean:
	rm --force -- ebin/*.beam Emakefile ebin/$(APP).app

ebin/$(APP).app: src/$(APP).app.src
	mkdir --parents ebin
	cp --force -- $< $@

ifdef DEBUG
EXTRA_OPTS:=debug_info,
endif

ifdef TEST
EXTRA_OPTS:=$(EXTRA_OPTS) {d,'TEST', true},
endif

Emakefile: Emakefile.src
	sed "s/{{EXTRA_OPTS}}/$(EXTRA_OPTS)/" $< > $@

