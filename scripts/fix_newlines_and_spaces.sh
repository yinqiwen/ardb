#!/bin/bash
find src -regextype posix-extended -regex ".*\.(cpp|h|hpp)$" -exec perl -pi -e 's/\t/    /g' {} \;
find src -regextype posix-extended -regex ".*\.(cpp|h|hpp)$" -exec perl -pi -e 's/[ \t]+$//g' {} \;
find -regextype posix-extended -regex ".*\.(cpp|h|hpp|cxx|c|pro|pri|sh|py|pl|mk)$" -print0 \
| perl -wn0e 'next if (m#^./deps/#); print' \
| xargs -0 -- fromdos -a -e -v --
