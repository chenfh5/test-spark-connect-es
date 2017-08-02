mvn install:install-file \
  -Dfile=elasticsearch-analysis-ik-1.10.2.jar \
  -DgroupId=org.elasticsearch \
  -DartifactId=elasticsearch-analysis-ik \
  -Dversion=1.10.2 \
  -Dpackaging=jar \
  -DgeneratePom=true

mvn install:install-file \
  -Dfile=elasticsearch-analysis-pinyin-1.8.2.jar \
  -DgroupId=org.elasticsearch \
  -DartifactId=elasticsearch-analysis-pinyin \
  -Dversion=1.8.2 \
  -Dpackaging=jar \
  -DgeneratePom=true