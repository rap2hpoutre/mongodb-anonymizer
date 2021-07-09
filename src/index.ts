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
    fakerLocale: flags.string({
      char: "f",
      description: "faker locale (e.g: en, fr, de)",
    }),
    ignoreDocuments: flags.string({
      char: "i",
      description:
        "documents from these collections will be ignored (comma separated)",
    }),
  };
  async run() {
    const { flags } = this.parse(MongodbAnonymizer);
    if (!flags.uri || !flags.targetUri) {
      this.error(
        "You must specify a source and a target uri (type -h for help)"
      );
    }

    if (flags.fakerLocale) {
      faker.locale = flags.fakerLocale;
    }
    const ignoreDocuments = flags.ignoreDocuments?.split(",") || [];

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

      if (ignoreDocuments.includes(collectionName)) {
        this.log("Ignoring collection: " + collectionName);
        // drop collection if it exists
        if ((await targetDb.collection(collectionName).countDocuments()) > 0) {
          await targetDb.collection(collectionName).drop();
        }
        continue;
      }

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
        (item) =>
          !item.match(/^[a-z_]+\./gi) || item.startsWith(`${collectionName}.`)
      )
      .map((item) => ({
        field: item
          .replace(`${collectionName}.`, "")
          .replace(/:(?:.*)$/, "")
          .toLowerCase(),
        replacement: item.includes(":") ? item.replace(/^(?:.*):/, "") : null,
      }));
    const fieldsToAnonymize = keysToAnonymize.map((item) => item.field);
    this.log(`Fields to anonymize: ${fieldsToAnonymize}`);
    for (const document of collectionData) {
      const documentAnonymized = {};
      for (const key in document) {
        if (!document) continue;
        if (fieldsToAnonymize.includes(key.toLowerCase())) {
          documentAnonymized[key] = this.anonymizeValue(
            key.toLowerCase(),
            keysToAnonymize.find((item) => item.field === key.toLowerCase())
              ?.replacement
          );
        } else {
          documentAnonymized[key] = document[key];
        }
      }
      collectionDataAnonymized.push(documentAnonymized);
    }
    return collectionDataAnonymized;
  }

  anonymizeValue(key: any, replacement) {
    if (replacement) {
      // Anonymize when key is like: `email:faker.internet.email`
      if (replacement.startsWith("faker")) {
        const [_one, two, three] = replacement.split(".");
        if (!(two && three)) return replacement;
        return faker[two][three]();
      } else if (replacement === "[]") {
        return [];
      } else if (replacement === "{}") {
        return {};
      } else if (replacement.startsWith("[") || replacement.startsWith("{")) {
        return JSON.parse(decodeURIComponent(replacement));
      } else if (replacement === "null") {
        return null;
      }
      // Anonymize when key is like: `email:raph@example.org`
      return replacement;
    }
    if (key.includes("email")) return faker.internet.email().toLowerCase();
    if (key.includes("firstname")) return faker.name.firstName();
    if (key.includes("lastname")) return faker.name.lastName();
    if (key === "description") return faker.lorem.sentence();
    if (key.endsWith("address")) return faker.address.streetAddress();
    if (key.endsWith("city")) return faker.address.city();
    if (key.endsWith("country")) return faker.address.country();
    if (key.endsWith("phone")) return faker.phone.phoneNumber();
    if (key.endsWith("comment")) return faker.lorem.sentence();
    if (key.endsWith("date")) return faker.date.past();
    if (key.endsWith("name")) return faker.name.findName();
    return faker.random.word();
  }
}

export = MongodbAnonymizer;
