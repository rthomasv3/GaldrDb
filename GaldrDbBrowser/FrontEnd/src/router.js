import { createWebHistory, createRouter } from "vue-router";

const Home = () => import("./components/database/Home.vue");
const CollectionView = () => import("./components/collections/CollectionView.vue");

const routes = [
    { path: "/", component: Home },
    { path: "/collection/:name", component: CollectionView },
];

const router = createRouter({
    history: createWebHistory(),
    routes,
});

export default router;
