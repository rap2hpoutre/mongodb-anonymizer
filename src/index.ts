import { Command, flags } from "@oclif/command";
import { MongoClient } from "mongodb";
import * as faker from "faker";

class MongodbAnonymizer extends Command {
  static description = "describe the command here";

  static flags = {
    version: flags.version({ char: "v" }),
    help: flags.help({ char: "h" }),
    uri: flags.string({ char: "u", description: "mongodb source" }),
    targetUri: flags.string({ char: "t", description: "mongodb target" }),
    list: flags.string({
      char: "l",
      description: "list of columns to anonymize",
      default:
        "email,name,description,address,city,country,phone,comment,birthdate",
    }),
  };

  async run() {
    const { flags } = this.parse(MongodbAnonymizer);
    if (!flags.uri || !flags.targetUri) {
      this.error(
        "You must specify a source and a target uri (type -h for help)"
      );
    }

    this.log("Connecting to source…");
    const client = new MongoClient(flags.uri, { useUnifiedTopology: true });
    await client.connect();
    const db = client.db();

    this.log("Connecting to target…");
    const targetClient = new MongoClient(flags.targetUri, {
      useUnifiedTopology: true,
    });
    await targetClient.connect();
    const targetDb = targetClient.db();

    this.log("Getting collections…");
    const collections = await db.listCollections().toArray();
    this.log("Collections: " + collections.map((item) => item.name));

    this.log("Anonymizing collections…");
    for (const collection of collections) {
      const collectionName = collection.name;
      this.log("Anonymizing collection: " + collectionName);
      const collectionData = await db
        .collection(collectionName)
        .find()
        .toArray();
      const list = flags.list.split(",");
      const collectionDataAnonymized = await this.anonymizeCollection(
        collectionData,
        collectionName,
        list
      );

      this.log("Inserting collection in target: " + collectionName);
      await targetDb.collection(collectionName).deleteMany({});
      // Save collection
      await targetDb
        .collection(collectionName)
        .insertMany(collectionDataAnonymized);
    }
    this.log("Done!");

    await client.close();
    await targetClient.close();
  }
  async anonymizeCollection(
    collectionData: any,
    collectionName: any,
    list: string[]
  ) {
    const collectionDataAnonymized = [];
    const keysToAnonymize = list
      .filter(
        (item) => !item.includes(".") || item.startsWith(`${collectionName}.`)
      )
      .map((item) => item.replace(`${collectionName}.`, ""));
    this.log(`Keys to anonymize: ${keysToAnonymize}`);
    for (const document of collectionData) {
      const documentAnonymized = {};
      for (const key in document) {
        if (!document) continue;
        if (keysToAnonymize.includes(key)) {
          documentAnonymized[key] = this.anonymizeValue(
            document[key],
            key.toLowerCase()
          );
        } else {
          documentAnonymized[key] = document[key];
        }
      }
      collectionDataAnonymized.push(documentAnonymized);
    }
    return collectionDataAnonymized;
  }

  anonymizeValue(value: any, key: any) {
    if (key.includes("email")) return faker.internet.email();
    if (key.endsWith("name")) return faker.name.findName();
    if (key.includes("firstName")) return faker.name.firstName();
    if (key.includes("lastName")) return faker.name.lastName();
    if (key === "description") return faker.lorem.sentence();
    if (key.endsWith("address")) return faker.address.streetAddress();
    if (key.endsWith("city")) return faker.address.city();
    if (key.endsWith("country")) return faker.address.country();
    if (key.endsWith("phone")) return faker.phone.phoneNumber();
    if (key.endsWith("comment")) return faker.lorem.sentence();
    if (key.endsWith("date")) return faker.date.past();
    return faker.random.word();
  }
}

export = MongodbAnonymizer;
