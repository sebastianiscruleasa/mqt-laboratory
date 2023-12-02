1. Creare Producer Kafka din consola (terminal).

2. Creare Consumer Kafka din consola (terminal).

3. Creare ProducerAPI care trimite evenimente sub forma de siruri de cacactere pe Kafka.

4. Creare ConsumerAPI Kafka ptr Producer-ul de mai sus.

5. Creare ProducerAPI care trimite evenimente sub forma json pe Kafka.

6. Creare ConsumerAPI Kafka ptr Producer-ul de mai sus.

5. Creare ProducerAPI care trimite evenimente sub forma de obiecte definite (clase Java, C#, etc) in API pe Kafka.

6. Creare ConsumerAPI Kafka ptr Producer-ul de mai sus.

7. Creare ProducerAPI si publicare mesaje folosind schema for Kafka.

8. Creare ConsumerAPI for Kafka si consumare mesaje folosind schema.

9. Creare pipeline: un ConsumerAPI poate deveni mai departe Producator.

10. Folositi: Confluent REST proxy si prin cereri HTTP: creati un topic, cititi mesajele din topic.

11. Creare API care publica evenimente pe 2 topic-uri Kafka. Folosind Kafka Streams, agregati datele de pe cele 2 topic-uri in alt topic.

12. Creare API care publica evenimente pe 2 topic-uri Kafka. Folosind kSQL efectuati cereri SQL folosind datele din cele 2 topic-uri.

13. Folosind un source Kafka connector cititi datele dintr-o tabele a unei baza de date (la alegere) si publicati-le pe un Kakfa topic.

14. Folosind un sink Kafka connector cititi datele dintr-un topic si publicati-le intr-o baza de date (la alegere).

15. Avand la dispozitie un fisier de mari dimensiuni, parsati-l folosind Spark RDDs: numarati de cate ori apare cuvantul "master" in fisier.

16. Avand la dispozitie un fisier de mari dimensiuni, parsati-l folosind Spark RDDs: afisati toate valorile numerice din fisier.

17. Avand la dispozitie doua fisiere de mari dimensiuni, folosind Spark RDDs parsati-l. Fisierul 1 contine cuvintele unui articol.
Fisierul 2 contine cuvinte uzuale, boring. In urma analizei celor 2 fisiere generati un rezultat, in forma de fisier, care reprezinta 
cuvintele din primul fisier, fara prezenta cuvintelor din al doilea fisier. Un fisier fara "boring" words. 