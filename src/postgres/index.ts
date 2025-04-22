#!/usr/bin/env node

import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import {
  CallToolRequestSchema,
  ListResourcesRequestSchema,
  ListToolsRequestSchema,
  ReadResourceRequestSchema,
} from "@modelcontextprotocol/sdk/types.js";
import pg from "pg";

const server = new Server(
  {
    name: "example-servers/postgres",
    version: "0.1.0",
  },
  {
    capabilities: {
      resources: {},
      tools: {},
    },
  }
);

const args = process.argv.slice(2);
if (args.length === 0) {
  console.error("Please provide a database URL as a command-line argument");
  process.exit(1);
}

const databaseUrl = args[0];

const resourceBaseUrl = new URL(databaseUrl);
resourceBaseUrl.protocol = "postgres:";
resourceBaseUrl.password = "";

const pool = new pg.Pool({
  connectionString: databaseUrl,
});

const SCHEMA_PATH = "schema";

server.setRequestHandler(ListResourcesRequestSchema, async () => {
  const client = await pool.connect();
  try {
    const result = await client.query(
      "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
    );
    return {
      resources: result.rows.map((row) => ({
        uri: new URL(`${row.table_name}/${SCHEMA_PATH}`, resourceBaseUrl).href,
        mimeType: "application/json",
        name: `"${row.table_name}" database schema`,
      })),
    };
  } finally {
    client.release();
  }
});

server.setRequestHandler(ReadResourceRequestSchema, async (request) => {
  const resourceUrl = new URL(request.params.uri);

  const pathComponents = resourceUrl.pathname.split("/");
  const schema = pathComponents.pop();
  const tableName = pathComponents.pop();

  if (schema !== SCHEMA_PATH) {
    throw new Error("Invalid resource URI");
  }

  const client = await pool.connect();
  try {
    const result = await client.query(
      "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = $1",
      [tableName]
    );

    return {
      contents: [
        {
          uri: request.params.uri,
          mimeType: "application/json",
          text: JSON.stringify(result.rows, null, 2),
        },
      ],
    };
  } finally {
    client.release();
  }
});

server.setRequestHandler(ListToolsRequestSchema, async () => {
  return {
    tools: [
      {
        name: "query",
        description: "Run a read-only SQL query",
        inputSchema: {
          type: "object",
          properties: {
            sql: { type: "string" },
          },
        },
      },
      {
        name: "create",
        description: "Run a create-table SQL query",
        inputSchema: {
          type: "object",
          properties: {
            sql: { type: "string" },
          },
        },
      },
      {
        name: "insert",
        description: "Run an insert-data SQL query",
        inputSchema: {
          type: "object",
          properties: {
            sql: { type: "string" },
          },
        },
      },
      {
        name: "update",
        description: "Run an update-data SQL query",
        inputSchema: {
          type: "object",
          properties: {
            sql: { type: "string" },
          },
        },
      },
      {
        name: "delete",
        description: "Run a delete-data SQL query",
        inputSchema: {
          type: "object",
          properties: {
            sql: { type: "string" },
          },
        },
      },
    ],
  };
});

server.setRequestHandler(CallToolRequestSchema, async (request) => {
  if (request.params.name === "query") {
    const sql = request.params.arguments?.sql as string;

    const client = await pool.connect();
    try {
      await client.query("BEGIN TRANSACTION READ ONLY");
      const result = await client.query(sql);
      return {
        content: [{ type: "text", text: JSON.stringify(result.rows, null, 2) }],
        isError: false,
      };
    } catch (error) {
      throw error;
    } finally {
      client
        .query("ROLLBACK")
        .catch((error) =>
          console.warn("Could not roll back transaction:", error)
        );

      client.release();
    }
  }
  if (request.params.name === "create") {
    const sql = request.params.arguments?.sql as string;
    const client = await pool.connect();
    try {
      await client.query("BEGIN TRANSACTION");
      await client.query(sql);
      await client.query("COMMIT");
      return {
        content: [{ type: "text", text: "Table created successfully" }],
        isError: false,
      };
    } catch (error) {
      await client
        .query("ROLLBACK")
        .catch((rollbackError) =>
          console.warn("Could not roll back transaction:", rollbackError)
        );
      throw error;
    } finally {
      client.release();
    }
  }
  if (request.params.name === "insert") {
    const sql = request.params.arguments?.sql as string;
    const client = await pool.connect();
    try {
      await client.query("BEGIN TRANSACTION");
      await client.query(sql);
      return {
        content: [{ type: "text", text: "Data inserted successfully" }],
        isError: false,
      };
    } catch (error) {
      throw error;
    } finally {
      client
        .query("ROLLBACK")
        .catch((error) =>
          console.warn("Could not roll back transaction:", error)
        );

      client.release();
    }
  }
  if (request.params.name === "update") {
    const sql = request.params.arguments?.sql as string;
    const client = await pool.connect();
    try {
      await client.query("BEGIN TRANSACTION");
      await client.query(sql);
      await client.query("COMMIT");
      return {
        content: [{ type: "text", text: "Data updated successfully" }],
        isError: false,
      };
    } catch (error) {
      await client
        .query("ROLLBACK")
        .catch((rollbackError) =>
          console.warn("Could not roll back transaction:", rollbackError)
        );
      throw error;
    } finally {
      client.release();
    }
  }
  if (request.params.name === "delete") {
    const sql = request.params.arguments?.sql as string;
    const client = await pool.connect();
    try {
      await client.query("BEGIN TRANSACTION");
      await client.query(sql);
      return {
        content: [{ type: "text", text: "Data deleted successfully" }],
        isError: false,
      };
    } catch (error) {
      throw error;
    } finally {
      client
        .query("ROLLBACK")
        .catch((error) =>
          console.warn("Could not roll back transaction:", error)
        );

      client.release();
    }
  }
  throw new Error(`Unknown tool: ${request.params.name}`);
});

async function runServer() {
  const transport = new StdioServerTransport();
  await server.connect(transport);
}

runServer().catch(console.error);
