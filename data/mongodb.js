db.createUser( {
    user: "predmain",
    pwd: passwordPrompt(),
    roles: [
      { role: "dbOwner", db: "predmain" }
    ]
} )

// Exporting data
mongoexport --out=devices.json --db=predmain --collection=devices -h localhost:27017 -u predmain -p $PASSWORD

// Importing data
mongoimport --file devices.json --db=predmain --collection=devices -h nnrtbqrbdeylh1o-loic.adb-preprod.us-phoenix-1.oraclecloudapps.com:27016 -u predmain -p $PASSWORD --ssl --tlsInsecure --authenticationMechanism=PLAIN
