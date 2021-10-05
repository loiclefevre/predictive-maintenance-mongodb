-- mongo --shell 'mongodb://predmain@132.145.157.51:27017/predmain?authenticationDatabase=admin'

db.createUser( {
    user: "predmain",
    pwd: passwordPrompt(),
    roles: [
      { role: "dbOwner", db: "predmain" }
    ]
} )


