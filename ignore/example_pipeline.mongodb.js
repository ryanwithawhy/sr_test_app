// let source_with_sr = {
//   "$source": {
//     "connectionName": "kafka-connection",
//     "topic" : ["topic-1", "topic-2", "topic-3"],
//     "schemaRegistry": {
//       "connectionName": "schema-registry-connection-name",
//       "onError": "dlq",
//       // [other optional options if desired]
//     }
//   }
// }

// let emit = {
//   $emit: {
//     "connectionName": "kafkaConnection",
//     "topic": "solar_generator_subcomponents_json", // string or an expression
// 	  "schemaRegistryConnectionName": "schema-registry-connection-name", 
//     "schemaValue": { // schemaValue is an object or an expression
//     "type": "type",
// ... [other options]
//   }
//     }
// }


// touch avro/user_v1.avsc
// touch avro/user_v2_add_optional.avsc
// touch avro/user_v3_remove_required.avsc
// touch avro/metadata.avsc
// touch avro/user_with_meta.avsc
// touch json/user_v1.json
// touch json/user_v2_add_optional.json