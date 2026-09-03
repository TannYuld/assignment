import { CacheType, RIDBHandle, type RIDBSchema } from "@tannyuld/ridbf";

const BLOGPOST_KEY: string = "blogposts";

export interface BlogPost {
    title: string,
    date: Date,
    tags?: string[],
    content: string
}

export const BlogPostSchema: RIDBSchema<BlogPost> = [
    { title: { unique: false } },
    "date",
    "tags",
    "content",
] as const;

export function retrieveData(): BlogPost[] {
    const retrievedData: string | null = localStorage.getItem(BLOGPOST_KEY);
    if (retrievedData === null) {
        return [];
    }

    const result: BlogPost[] = JSON.parse(retrievedData as string);
    result.map(post => {
        if (typeof post.date !== typeof Date) {
            post.date = new Date(post.date);
        }
        return post;
    });
    return result;
}

export async function fetchDataIfIntegrityNotMatch(): Promise<BlogPost[]> {
    const handle = RIDBHandle.open("blogpost", BlogPostSchema, { dataCache: CacheType.NoCache, integrityCache: CacheType.NoCache });
    await handle.fetch();
    const result = await handle.findAll();
    if (result !== undefined || result !== null) {
        localStorage.setItem(BLOGPOST_KEY, JSON.stringify(result));
    }
    return result;
}