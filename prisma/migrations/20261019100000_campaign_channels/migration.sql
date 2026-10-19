-- Per-campaign channel switches. Existing campaigns keep answering everywhere.
ALTER TABLE "Automation" ADD COLUMN "instagramEnabled" BOOLEAN NOT NULL DEFAULT true;
ALTER TABLE "Automation" ADD COLUMN "facebookEnabled" BOOLEAN NOT NULL DEFAULT true;
