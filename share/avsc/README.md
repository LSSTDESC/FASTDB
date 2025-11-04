## lsst namespaces

`lsst.v9_0.*` was copied directly from https://github.com/lsst/alert_packet/blob/main/python/lsst/alert/packet/schema/9/0/lsst.v9_0.alert.avsc

## fastdb namespaces

### fastdb_9_0_2

Based on `lsst.v9_0`.  Has only `BrokerMessage` in it, which includes things from the `lsstv9_0` namespace.

The **BrokerMessage** schema is just the LSST v9.0 alert schema plus four additional fields:

* `brokerName` identifies the broker that provided the classifications (should be one of ANTARES, ALcRCE, AMPEL, FINK, Lasair, Pitt-Google, or Babmul, but is sometimes something else in tests).

* `classifierName` identifies the classifier that the broker used.

* `classifierVersion` identifies the version of the classifier that the broker used.  Ideally, if the training of a classifier changes, this version should bump.  This doesn't strictly have to be a semantic version; it can also include paramters used for the classifier.  (E.g., it could be something like "v1.0.0 with param_1=val_1, param_2=val_2".)

* `classifications` is an array of classifications.  Each element of the array is a two-element record, an integer with a class ID, and a float with a probability between 0 and 1.  classId values will be taken from the [ELAsTiCC2 taxnomy]() (though this may evolve).  Probabilities, ideally, sum to 1.  Use the classID 200 ("Residual") if the probabilities don't sum to 1.  (So, for example, if all you have is a classifier that decides the probability of it being a SNIa, and a given candidate has a 25% probability of being a SNIa, then the `classificatations` field should hold `[{2222,0.25},{200,0.75}]`.)

### fastdb_test_0.2

**This one is old and out of date, ignore it.**

`[namespace].Alert.avsc` is manually constructed

`[namespace].Dia*.avsc` were produced by the code in `src/admin` under repo root

`[namespace].BrokerMesssage.avsc is copied from Alert.avsc, but has the added "classifications" field


