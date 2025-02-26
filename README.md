# kafka-platforce

App for å importere relevante data fra Kafka til Salesforce.
Én app-instans per Kafka-kø.
For eksempel:
* sf-pdl-kafka
* sf-geografisktilknytning

Kafka-eventer kan filtreres og modifiseres programmatisk per app, se [Bootstrap.kt](https://github.com/navikt/sf-pdl-kafka/blob/master/src/main/kotlin/no/nav/sf/pdl/kafka/Bootstrap.kt).

Spesifikk NAIS-konfigurasjon per app ligger under [.nais](https://github.com/navikt/sf-pdl-kafka/tree/master/.nais).

For eksempel, for sf-pdl-kafka har vi et whitelist-filter her:
[resources/whitelist](https://github.com/navikt/sf-pdl-kafka/tree/master/src/main/resources/whitelist)

Du finner en visualisering av hvilke felt som blir blokkert pa denne ingress:
[sf-pdl-kafka.intern.nav.no/internal/gui](https://sf-pdl-kafka.intern.nav.no/internal/gui)


# NB! Deploy vid push styrd av main.yaml
Merk at statusen til [main.yaml](https://github.com/navikt/sf-pdl-kafka/blob/master/.github/workflows/main.yml) bestemmer hvilken app som blir deployet og hvor ved push.

Hvis du bruker main.yml for kjøringer med flagg satt til TRUE (FLAG_SEEK, FLAG_NO_POST, FLAG_ALT_ID), husk å sette dem tilbake til FALSE når du er ferdig.

Alternativt kan du bruke dispatch-jobben for kjøringer med flagg satt til TRUE, slik at de kun er aktive for den spesifikke deployen.

En beskrivelse av hva flaggene gjør finner du i [main.yaml](https://github.com/navikt/sf-pdl-kafka/blob/master/.github/workflows/main.yml).
