package org.example.demo2

object Schema {
  val nodeSchema = Map(
    "Person" -> Array(
      ("id:ID", "BIGINT"),
      (":LABEL", "String"),
      ("creationDate", "Date"),
      ("firstName", "String"),
      ("lastName", "String"),
      ("gender", "String"),
      ("birthday", "Date"),
      ("locationIP", "String"),
      ("browserUsed", "String"),
      ("languages", "String"),
      ("emails", "String")),
    "Place" -> Array(
      ("id:ID", "BIGINT"),
      (":LABEL", "String"),
      ("name", "String"),
      ("url", "String"),
      ("type", "String")),
    "Organisation" -> Array(
      ("id:ID", "BIGINT"),
      (":LABEL", "String"),
      ("type", "String"),
      ("name", "String"),
      ("url", "String")),
    "Comment" -> Array(
      ("id:ID", "BIGINT"),
      (":LABEL", "String"),
      ("creationDate", "Date"),
      ("locationIP", "String"),
      ("browserUsed", "String"),
      ("content", "String"),
      ("length", "INT")),
    "Post" -> Array(
      ("id:ID", "BIGINT"),
      ("creationDate", "Date"),
      (":LABEL", "String"),
      ("imageFile", "String"),
      ("locationIP", "String"),
      ("browserUsed", "String"),
      ("language", "String"),
      ("content", "String"),
      ("length", "INT")),
    "Forum" -> Array(
      ("id:ID", "BIGINT"),
      ("creationDate", "Date"),
      (":LABEL", "String"),
      ("title", "String")),
    "Tag" -> Array(
      ("id:ID", "BIGINT"),
      (":LABEL", "String"),
      ("name", "String"),
      ("url", "String")),
    "Tagclass" -> Array(
      ("id:ID", "BIGINT"),
      (":LABEL", "String"),
      ("name", "String"),
      ("url", "String"))
  )

  val relSchema = Map(
    "knows" -> Array(("REL_ID", "BIGINT"), (":TYPE", "String"), ("creationDate", "Date"), (":START_ID", "BIGINT"), (":END_ID", "BIGINT")),
    "isLocatedIn" -> Array(("REL_ID", "BIGINT"), (":TYPE", "String"), (":START_ID", "BIGINT"), (":END_ID", "BIGINT"), ("creationDate", "Date")),
    "containerOf" -> Array(("REL_ID", "BIGINT"), (":TYPE", "String"), ("creationDate", "Date"), (":START_ID", "BIGINT"), (":END_ID", "BIGINT")),
    "hasCreator" -> Array(("REL_ID", "BIGINT"), (":TYPE", "String"), ("creationDate", "Date"), (":START_ID", "BIGINT"), (":END_ID", "BIGINT")),
    "hasInterest" -> Array(("REL_ID", "BIGINT"), (":TYPE", "String"), ("creationDate", "Date"), (":START_ID", "BIGINT"), (":END_ID", "BIGINT")),
    "hasMember" -> Array(("REL_ID", "BIGINT"), (":TYPE", "String"), ("creationDate", "Date"), (":START_ID", "BIGINT"), (":END_ID", "BIGINT")),
    "hasModerator" -> Array(("REL_ID", "BIGINT"), (":TYPE", "String"), ("creationDate", "Date"), (":START_ID", "BIGINT"), (":END_ID", "BIGINT")),
    "hasTag" -> Array(("REL_ID", "BIGINT"), (":TYPE", "String"), ("creationDate", "Date"), (":START_ID", "BIGINT"), (":END_ID", "BIGINT")),
    "hasType" -> Array(("REL_ID", "BIGINT"), (":TYPE", "String"), (":START_ID", "BIGINT"), (":END_ID", "BIGINT")),
    "isPartOf" -> Array(("REL_ID", "BIGINT"), (":TYPE", "String"), (":START_ID", "BIGINT"), (":END_ID", "BIGINT")),
    "isSubclassOf" -> Array(("REL_ID", "BIGINT"), (":TYPE", "String"), (":START_ID", "BIGINT"), (":END_ID", "BIGINT")),
    "likes" -> Array(("REL_ID", "BIGINT"), (":TYPE", "String"), ("creationDate", "Date"), (":START_ID", "BIGINT"), (":END_ID", "BIGINT")),
    "replyOf" -> Array(("REL_ID", "BIGINT"), (":TYPE", "String"), ("creationDate", "Date"), (":START_ID", "BIGINT"), (":END_ID", "BIGINT")),
    "studyAt" -> Array(("REL_ID", "BIGINT"), (":TYPE", "String"), ("creationDate", "Date"), (":START_ID", "BIGINT"), (":END_ID", "BIGINT"), ("classYear", "INT")),
    "workAt" -> Array(("REL_ID", "BIGINT"), (":TYPE", "String"), ("creationDate", "Date"), (":START_ID", "BIGINT"), (":END_ID", "BIGINT"), ("workFrom", "INT"))
  )

  val relMapping = Map(
    "isLocatedIn" -> (Array("Person", "Comment", "Post", "Organisation"), Array("Place")),
    "replyOf" -> (Array("Comment"), Array("Comment", "Post")), "containerOf" -> (Array("Forum"), Array("Post")),
    "hasCreator" -> (Array("Comment", "Post"), Array("Person")), "hasInterest" -> (Array("Person"), Array("Tag")),
    "workAt" -> (Array("Person"), Array("Organisation")), "hasModerator" -> (Array("Forum"), Array("Person")),
    "hasTag" -> (Array("Comment", "Post", "Forum"), Array("Tag")), "hasType" -> (Array("Tag"), Array("Tagclass")),
    "isSubclassOf" -> (Array("Tagclass"), Array("Tagclass")), "isPartOf" -> (Array("Place"), Array("Place")),
    "likes" -> (Array("Person"), Array("Comment", "Post")), "knows" -> (Array("Person"), Array("Person")),
    "studyAt" -> (Array("Person"), Array("Organisation")), "hasMember" -> (Array("Forum"), Array("Person")),
  )

}
