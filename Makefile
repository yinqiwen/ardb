default: all


deb:		
	dpkg-buildpackage

.DEFAULT:
	cd src && $(MAKE) $@
