import "server-only";
import { promises as fs } from "fs";
import path from "path";
import { randomUUID } from "crypto";
import type { AppUser, Item } from "./types";
import { SEED_ITEMS } from "./seed";

// Users and projects live here. Production uses Firestore in the sigfit
// Google Cloud project; local development uses a JSON file.

export interface Store {
  getUser(email: string): Promise<AppUser | null>;
  listUsers(): Promise<AppUser[]>;
  saveUser(user: AppUser): Promise<void>;
  deleteUser(email: string): Promise<void>;
  listItems(): Promise<Item[]>;
  getItem(id: string): Promise<Item | null>;
  saveItem(item: Item): Promise<void>;
}

export const newId = () => randomUUID();

// ---------------------------------------------------------------- file store

interface FileData {
  users: Record<string, AppUser>;
  items: Record<string, Item>;
}

class FileStore implements Store {
  private file = path.resolve(process.env.DATA_FILE || ".data/store.json");
  private chain: Promise<unknown> = Promise.resolve();

  private async read(): Promise<FileData> {
    try {
      return JSON.parse(await fs.readFile(this.file, "utf8"));
    } catch {
      return { users: {}, items: {} };
    }
  }

  private write(mutate: (d: FileData) => void): Promise<void> {
    const next = this.chain.then(async () => {
      const data = await this.read();
      mutate(data);
      await fs.mkdir(path.dirname(this.file), { recursive: true });
      await fs.writeFile(this.file, JSON.stringify(data, null, 2));
    });
    this.chain = next.catch(() => undefined);
    return next;
  }

  async getUser(email: string) {
    return (await this.read()).users[email.toLowerCase()] ?? null;
  }
  async listUsers() {
    return Object.values((await this.read()).users);
  }
  saveUser(user: AppUser) {
    return this.write((d) => void (d.users[user.email] = user));
  }
  deleteUser(email: string) {
    return this.write((d) => void delete d.users[email.toLowerCase()]);
  }
  async listItems() {
    return Object.values((await this.read()).items);
  }
  async getItem(id: string) {
    return (await this.read()).items[id] ?? null;
  }
  saveItem(item: Item) {
    return this.write((d) => void (d.items[item.id] = item));
  }
}

// ----------------------------------------------------------- firestore store

class FirestoreStore implements Store {
  // Loaded lazily so local development never needs Google credentials.
  private dbPromise = import("@google-cloud/firestore").then(
    ({ Firestore }) =>
      new Firestore({
        projectId: process.env.FIRESTORE_PROJECT || process.env.BQ_PROJECT || "sigfit",
        databaseId: process.env.FIRESTORE_DATABASE || "(default)",
        ignoreUndefinedProperties: true,
      }),
  );

  private async col(name: "users" | "items") {
    return (await this.dbPromise).collection(name);
  }
  async getUser(email: string) {
    const snap = await (await this.col("users")).doc(email.toLowerCase()).get();
    return snap.exists ? (snap.data() as AppUser) : null;
  }
  async listUsers() {
    const snap = await (await this.col("users")).get();
    return snap.docs.map((d) => d.data() as AppUser);
  }
  async saveUser(user: AppUser) {
    await (await this.col("users")).doc(user.email).set(user);
  }
  async deleteUser(email: string) {
    await (await this.col("users")).doc(email.toLowerCase()).delete();
  }
  async listItems() {
    const snap = await (await this.col("items")).get();
    return snap.docs.map((d) => d.data() as Item);
  }
  async getItem(id: string) {
    const snap = await (await this.col("items")).doc(id).get();
    return snap.exists ? (snap.data() as Item) : null;
  }
  async saveItem(item: Item) {
    await (await this.col("items")).doc(item.id).set(item);
  }
}

// ------------------------------------------------------------------ singleton

const g = globalThis as unknown as { __sfStore?: Store; __sfSeeded?: Promise<void> };

export async function store(): Promise<Store> {
  if (!g.__sfStore) {
    g.__sfStore = process.env.STORE === "file" ? new FileStore() : new FirestoreStore();
  }
  g.__sfSeeded ??= seed(g.__sfStore);
  await g.__sfSeeded;
  return g.__sfStore;
}

/** First run only: add the starting projects so the board isn't empty. */
async function seed(s: Store) {
  const items = await s.listItems();
  if (items.length > 0) return;
  const now = new Date().toISOString();
  for (const [i, seedItem] of SEED_ITEMS.entries()) {
    await s.saveItem({
      id: newId(),
      description: "",
      visibility: "team",
      createdBy: "system",
      createdAt: now,
      updatedAt: now,
      watchers: [],
      updates: [],
      rank: i,
      ...seedItem,
    });
  }
}
